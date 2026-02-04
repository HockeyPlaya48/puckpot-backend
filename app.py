"""
PuckPot Backend API
NHL Pick'em game backend with NHL API integration, database storage, and contract interaction.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
import httpx
import os
import json
from dotenv import load_dotenv

# Database imports
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Background scheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Web3 for contract interaction
from web3 import Web3

load_dotenv()

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./puckpot.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# NHL API Base URL
NHL_API_BASE = "https://api-web.nhle.com/v1"

# Contract configuration
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "")
BASE_RPC_URL = os.getenv("BASE_RPC_URL", "https://mainnet.base.org")

# Web3 setup
w3 = Web3(Web3.HTTPProvider(BASE_RPC_URL))

# Contract ABI (minimal for reading)
PUCKPOT_ABI = [
    {
        "inputs": [{"name": "contestId", "type": "uint256"}],
        "name": "getContest",
        "outputs": [
            {"name": "lockTime", "type": "uint256"},
            {"name": "totalPot", "type": "uint256"},
            {"name": "state", "type": "uint8"},
            {"name": "numGames", "type": "uint8"},
            {"name": "entrantCount", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"name": "contestId", "type": "uint256"}],
        "name": "getWinners",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"name": "contestId", "type": "uint256"}, {"name": "player", "type": "address"}],
        "name": "hasEntered",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function"
    }
]

# Database Models
class ContestDB(Base):
    __tablename__ = "contests"

    id = Column(Integer, primary_key=True)  # YYYYMMDD format
    date = Column(String, index=True)
    lock_time = Column(DateTime)
    games = Column(JSON)  # List of game data
    created_at = Column(DateTime, default=datetime.utcnow)

class EntryDB(Base):
    __tablename__ = "entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contest_id = Column(Integer, index=True)
    wallet_address = Column(String, index=True)
    picks = Column(JSON)
    tiebreaker_guess = Column(Integer)
    tx_hash = Column(String)
    submitted_at = Column(DateTime, default=datetime.utcnow)

class GameResultDB(Base):
    __tablename__ = "game_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contest_id = Column(Integer, index=True)
    game_index = Column(Integer)
    nhl_game_id = Column(Integer)
    home_score = Column(Integer)
    away_score = Column(Integer)
    winner = Column(Integer)  # 0=away, 1=home, 2=cancelled
    is_final = Column(Boolean, default=False)
    updated_at = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(bind=engine)

# Pydantic Models
class Game(BaseModel):
    nhl_game_id: int
    game_index: int
    home_team: str
    away_team: str
    home_team_abbrev: str
    away_team_abbrev: str
    start_time: datetime
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: str  # SCHEDULED, LIVE, FINAL, POSTPONED
    period: Optional[str] = None

class Contest(BaseModel):
    contest_id: int
    date: str
    lock_time: datetime
    games: List[Game]
    total_pot: float
    entrant_count: int
    state: str  # open, locked, settled

class EntrySubmission(BaseModel):
    wallet_address: str
    contest_id: int
    picks: List[int]
    tiebreaker_guess: int
    tx_hash: str

class LeaderboardEntry(BaseModel):
    wallet_address: str
    correct_picks: int
    tiebreaker_guess: int
    tiebreaker_diff: Optional[int] = None
    rank: Optional[int] = None

# Scheduler
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    scheduler.add_job(update_live_scores, 'interval', minutes=1, id='live_scores')
    scheduler.add_job(check_contests_to_settle, 'interval', minutes=5, id='check_settle')
    scheduler.start()
    yield
    # Shutdown
    scheduler.shutdown()

app = FastAPI(title="PuckPot API", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://*.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# NHL API Functions
async def fetch_nhl_schedule(date: str = None) -> dict:
    """Fetch NHL schedule for a specific date"""
    async with httpx.AsyncClient() as client:
        if date:
            url = f"{NHL_API_BASE}/schedule/{date}"
        else:
            url = f"{NHL_API_BASE}/schedule/now"
        response = await client.get(url, timeout=10.0)
        return response.json()

async def fetch_nhl_scores(date: str = None) -> dict:
    """Fetch live/final scores for games"""
    async with httpx.AsyncClient() as client:
        if date:
            url = f"{NHL_API_BASE}/score/{date}"
        else:
            url = f"{NHL_API_BASE}/score/now"
        response = await client.get(url, timeout=10.0)
        return response.json()

def parse_games_from_schedule(schedule_data: dict, target_date: str) -> List[Game]:
    """Parse NHL schedule into Game objects"""
    games = []

    game_week = schedule_data.get("gameWeek", [])
    for day in game_week:
        if day.get("date") == target_date:
            for idx, game in enumerate(day.get("games", [])):
                # Determine status
                game_state = game.get("gameState", "FUT")
                if game_state == "FUT" or game_state == "PRE":
                    status = "SCHEDULED"
                elif game_state == "LIVE" or game_state == "CRIT":
                    status = "LIVE"
                elif game_state == "FINAL" or game_state == "OFF":
                    status = "FINAL"
                elif game_state == "PPD":
                    status = "POSTPONED"
                else:
                    status = "SCHEDULED"

                # Get period info
                period = None
                if status == "LIVE":
                    period_info = game.get("periodDescriptor", {})
                    period = period_info.get("periodType", "")

                games.append(Game(
                    nhl_game_id=game.get("id", 0),
                    game_index=idx,
                    home_team=game.get("homeTeam", {}).get("placeName", {}).get("default", "Unknown"),
                    away_team=game.get("awayTeam", {}).get("placeName", {}).get("default", "Unknown"),
                    home_team_abbrev=game.get("homeTeam", {}).get("abbrev", "???"),
                    away_team_abbrev=game.get("awayTeam", {}).get("abbrev", "???"),
                    start_time=datetime.fromisoformat(
                        game.get("startTimeUTC", "").replace("Z", "+00:00")
                    ) if game.get("startTimeUTC") else datetime.now(timezone.utc),
                    home_score=game.get("homeTeam", {}).get("score"),
                    away_score=game.get("awayTeam", {}).get("score"),
                    status=status,
                    period=period
                ))

    return sorted(games, key=lambda g: g.start_time)

async def get_contract_data(contest_id: int) -> dict:
    """Get contest data from the smart contract"""
    if not CONTRACT_ADDRESS:
        return {"total_pot": 0, "entrant_count": 0, "state": "open"}

    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=PUCKPOT_ABI
        )
        result = contract.functions.getContest(contest_id).call()

        state_map = {0: "open", 1: "locked", 2: "settled"}

        return {
            "total_pot": result[1] / 1_000_000,  # Convert from USDC units
            "entrant_count": result[4],
            "state": state_map.get(result[2], "open")
        }
    except Exception as e:
        print(f"Error fetching contract data: {e}")
        return {"total_pot": 0, "entrant_count": 0, "state": "open"}

# Background Jobs
async def update_live_scores():
    """Poll NHL API for live score updates"""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        scores = await fetch_nhl_scores(today)

        db = SessionLocal()
        contest_id = int(today.replace("-", ""))

        games = parse_games_from_schedule(scores, today)

        for game in games:
            # Update or create game result
            existing = db.query(GameResultDB).filter(
                GameResultDB.contest_id == contest_id,
                GameResultDB.nhl_game_id == game.nhl_game_id
            ).first()

            if game.status == "FINAL" and game.home_score is not None:
                winner = 1 if game.home_score > game.away_score else 0

                if existing:
                    existing.home_score = game.home_score
                    existing.away_score = game.away_score
                    existing.winner = winner
                    existing.is_final = True
                    existing.updated_at = datetime.utcnow()
                else:
                    db.add(GameResultDB(
                        contest_id=contest_id,
                        game_index=game.game_index,
                        nhl_game_id=game.nhl_game_id,
                        home_score=game.home_score,
                        away_score=game.away_score,
                        winner=winner,
                        is_final=True
                    ))

        db.commit()
        db.close()
    except Exception as e:
        print(f"Error updating live scores: {e}")

async def check_contests_to_settle():
    """Check for contests that can be settled"""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        contest_id = int(today.replace("-", ""))

        db = SessionLocal()
        contest = db.query(ContestDB).filter(ContestDB.id == contest_id).first()

        if not contest:
            db.close()
            return

        num_games = len(contest.games) if contest.games else 0
        final_results = db.query(GameResultDB).filter(
            GameResultDB.contest_id == contest_id,
            GameResultDB.is_final == True
        ).count()

        if final_results == num_games and num_games > 0:
            print(f"Contest {contest_id} ready for settlement - all {num_games} games final")
            # Settlement would be triggered here by the contract owner

        db.close()
    except Exception as e:
        print(f"Error checking contests: {e}")

# API Endpoints
@app.get("/")
async def root():
    return {"message": "PuckPot API", "version": "1.0.0"}

@app.get("/api/contests/today", response_model=Contest)
async def get_today_contest():
    """Get today's contest with games"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        schedule = await fetch_nhl_schedule(today)
        games = parse_games_from_schedule(schedule, today)

        if not games:
            raise HTTPException(status_code=404, detail="No games scheduled today")

        # Lock time = 5 minutes before first puck drop
        lock_time = games[0].start_time - timedelta(minutes=5)
        contest_id = int(today.replace("-", ""))

        # Store/update contest in database
        db = SessionLocal()
        contest_db = db.query(ContestDB).filter(ContestDB.id == contest_id).first()

        games_json = [g.model_dump(mode='json') for g in games]

        if not contest_db:
            contest_db = ContestDB(
                id=contest_id,
                date=today,
                lock_time=lock_time,
                games=games_json
            )
            db.add(contest_db)
            db.commit()
        else:
            contest_db.games = games_json
            contest_db.lock_time = lock_time
            db.commit()

        db.close()

        # Get contract data
        contract_data = await get_contract_data(contest_id)

        # Determine state
        now = datetime.now(timezone.utc)
        if contract_data["state"] == "settled":
            state = "settled"
        elif now >= lock_time:
            state = "locked"
        else:
            state = "open"

        return Contest(
            contest_id=contest_id,
            date=today,
            lock_time=lock_time,
            games=games,
            total_pot=contract_data["total_pot"],
            entrant_count=contract_data["entrant_count"],
            state=state
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching contest: {str(e)}")

@app.get("/api/contests/{contest_id}/games")
async def get_contest_games(contest_id: int):
    """Get games with current scores for a contest"""
    date = f"{str(contest_id)[:4]}-{str(contest_id)[4:6]}-{str(contest_id)[6:]}"

    try:
        scores = await fetch_nhl_scores(date)
        games = parse_games_from_schedule(scores, date)
        return {"games": games}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching games: {str(e)}")

@app.get("/api/contests/{contest_id}/leaderboard")
async def get_leaderboard(contest_id: int):
    """Get current leaderboard for a contest"""
    db = SessionLocal()

    try:
        entries = db.query(EntryDB).filter(EntryDB.contest_id == contest_id).all()
        results = db.query(GameResultDB).filter(
            GameResultDB.contest_id == contest_id,
            GameResultDB.is_final == True
        ).all()

        # Create results lookup
        results_map = {r.game_index: r.winner for r in results}

        # Get highest scoring game total for tiebreaker
        highest_total = 0
        for r in results:
            if r.home_score and r.away_score:
                total = r.home_score + r.away_score
                if total > highest_total:
                    highest_total = total

        leaderboard = []
        for entry in entries:
            correct = 0
            picks = entry.picks or []

            for idx, pick in enumerate(picks):
                if idx in results_map and results_map[idx] == pick:
                    correct += 1

            tiebreaker_diff = abs(entry.tiebreaker_guess - highest_total) if highest_total > 0 else None

            leaderboard.append(LeaderboardEntry(
                wallet_address=entry.wallet_address,
                correct_picks=correct,
                tiebreaker_guess=entry.tiebreaker_guess,
                tiebreaker_diff=tiebreaker_diff
            ))

        # Sort by correct picks (desc), then tiebreaker diff (asc)
        leaderboard.sort(key=lambda x: (-x.correct_picks, x.tiebreaker_diff or float('inf')))

        # Assign ranks
        for i, entry in enumerate(leaderboard):
            entry.rank = i + 1

        return {
            "leaderboard": leaderboard,
            "games_final": len(results),
            "highest_scoring_game_goals": highest_total
        }
    finally:
        db.close()

@app.post("/api/entries/record")
async def record_entry(entry: EntrySubmission):
    """Record a new entry after blockchain confirmation"""
    db = SessionLocal()

    try:
        # Check if entry already exists
        existing = db.query(EntryDB).filter(
            EntryDB.contest_id == entry.contest_id,
            EntryDB.wallet_address == entry.wallet_address.lower()
        ).first()

        if existing:
            raise HTTPException(status_code=400, detail="Entry already recorded")

        # Create entry
        new_entry = EntryDB(
            contest_id=entry.contest_id,
            wallet_address=entry.wallet_address.lower(),
            picks=entry.picks,
            tiebreaker_guess=entry.tiebreaker_guess,
            tx_hash=entry.tx_hash
        )
        db.add(new_entry)
        db.commit()

        return {"message": "Entry recorded successfully"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error recording entry: {str(e)}")
    finally:
        db.close()

@app.get("/api/user/{wallet_address}/history")
async def get_user_history(wallet_address: str):
    """Get user's contest history"""
    db = SessionLocal()

    try:
        entries = db.query(EntryDB).filter(
            EntryDB.wallet_address == wallet_address.lower()
        ).order_by(EntryDB.contest_id.desc()).limit(50).all()

        history = []
        for entry in entries:
            history.append({
                "contest_id": entry.contest_id,
                "picks": entry.picks,
                "tiebreaker_guess": entry.tiebreaker_guess,
                "submitted_at": entry.submitted_at.isoformat() if entry.submitted_at else None
            })

        return {"history": history}
    finally:
        db.close()

@app.get("/api/user/{wallet_address}/contests/{contest_id}")
async def get_user_entry(wallet_address: str, contest_id: int):
    """Get user's entry for a specific contest"""
    db = SessionLocal()

    try:
        entry = db.query(EntryDB).filter(
            EntryDB.contest_id == contest_id,
            EntryDB.wallet_address == wallet_address.lower()
        ).first()

        if not entry:
            raise HTTPException(status_code=404, detail="Entry not found")

        return {
            "contest_id": entry.contest_id,
            "picks": entry.picks,
            "tiebreaker_guess": entry.tiebreaker_guess,
            "tx_hash": entry.tx_hash,
            "submitted_at": entry.submitted_at.isoformat() if entry.submitted_at else None
        }
    finally:
        db.close()

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
