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
from zoneinfo import ZoneInfo
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

# NHL uses Eastern Time for scheduling — always calculate dates in ET
EASTERN = ZoneInfo("America/New_York")

def get_et_date(delta_days: int = 0) -> str:
    """Return YYYY-MM-DD in Eastern Time. delta_days=0 is today, -1 is yesterday."""
    return (datetime.now(EASTERN) + timedelta(days=delta_days)).strftime("%Y-%m-%d")

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
OWNER_PRIVATE_KEY = os.getenv("OWNER_PRIVATE_KEY", "")

# Web3 setup
w3 = Web3(Web3.HTTPProvider(BASE_RPC_URL))

# Get owner account from private key
owner_account = None
if OWNER_PRIVATE_KEY:
    owner_account = w3.eth.account.from_key(OWNER_PRIVATE_KEY)

# Contract ABI (includes settleContest for auto-settlement)
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
    },
    {
        "inputs": [
            {"name": "contestId", "type": "uint256"},
            {"name": "results", "type": "uint8[]"},
            {"name": "highestScoringGameGoals", "type": "uint256"}
        ],
        "name": "settleContest",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "contestId", "type": "uint256"},
            {"name": "lockTime", "type": "uint256"},
            {"name": "numGames", "type": "uint8"}
        ],
        "name": "createContest",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"name": "contestId", "type": "uint256"}],
        "name": "getEntrants",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "contestId", "type": "uint256"},
            {"name": "player", "type": "address"}
        ],
        "name": "getEntry",
        "outputs": [
            {"name": "picks", "type": "uint8[]"},
            {"name": "tiebreakerGuess", "type": "uint256"},
            {"name": "correctPicks", "type": "uint8"},
            {"name": "tiebreakerDiff", "type": "uint256"}
        ],
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
    scheduler.add_job(auto_create_daily_contest, 'interval', hours=1, id='auto_create')
    scheduler.add_job(sync_entries_from_blockchain, 'interval', minutes=2, id='sync_entries')
    scheduler.start()
    # Run auto-create and sync immediately on startup
    await auto_create_daily_contest()
    await sync_entries_from_blockchain()
    yield
    # Shutdown
    scheduler.shutdown()

async def sync_entries_from_blockchain():
    """Sync entries from blockchain to database (in case frontend recording failed)"""
    if not CONTRACT_ADDRESS:
        return

    try:
        today = get_et_date(0)
        yesterday = get_et_date(-1)

        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=PUCKPOT_ABI
        )

        # Try today's contest first, then yesterday's if today has no entries
        contest_ids_to_check = [
            int(today.replace("-", "")),
            int(yesterday.replace("-", ""))
        ]

        for contest_id in contest_ids_to_check:
            # Get all entrants from blockchain
            try:
                entrants = contract.functions.getEntrants(contest_id).call()
            except Exception as e:
                # Contest might not exist
                continue

            if not entrants:
                continue

            db = SessionLocal()

            for wallet_address in entrants:
                # Check if already in database
                existing = db.query(EntryDB).filter(
                    EntryDB.contest_id == contest_id,
                    EntryDB.wallet_address == wallet_address.lower()
                ).first()

                if existing:
                    continue

                # Get entry details from blockchain
                try:
                    entry_data = contract.functions.getEntry(contest_id, wallet_address).call()
                    picks = [int(p) for p in entry_data[0]]
                    tiebreaker_guess = int(entry_data[1])

                    # Record in database
                    new_entry = EntryDB(
                        contest_id=contest_id,
                        wallet_address=wallet_address.lower(),
                        picks=picks,
                        tiebreaker_guess=tiebreaker_guess,
                        tx_hash="synced_from_blockchain"
                    )
                    db.add(new_entry)
                    db.commit()
                    print(f"Synced entry from blockchain: {wallet_address} for contest {contest_id}")
                except Exception as e:
                    print(f"Error syncing entry for {wallet_address}: {e}")

            db.close()
    except Exception as e:
        print(f"Error syncing entries from blockchain: {e}")

async def auto_create_daily_contest():
    """Automatically create today's contest on the blockchain if it doesn't exist"""
    if not CONTRACT_ADDRESS or not owner_account:
        print("Cannot auto-create contest: CONTRACT_ADDRESS or OWNER_PRIVATE_KEY not configured")
        return

    try:
        today = get_et_date(0)
        contest_id = int(today.replace("-", ""))

        # Check if contest already exists on-chain
        contract_data = await get_contract_data(contest_id)
        if contract_data.get("entrant_count", 0) > 0 or contract_data.get("state") != "open":
            # Contest exists or has entries
            return

        # Fetch today's schedule
        schedule = await fetch_nhl_schedule(today)
        games = parse_games_from_schedule(schedule, today)

        if not games:
            print(f"No games scheduled for {today}, skipping contest creation")
            return

        # Check if contract has this contest
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=PUCKPOT_ABI
        )

        try:
            result = contract.functions.getContest(contest_id).call()
            lock_time_onchain = result[0]
            if lock_time_onchain > 0:
                print(f"Contest {contest_id} already exists on-chain")
                return
        except:
            pass  # Contest doesn't exist, we'll create it

        # Lock time = 5 minutes before first game
        lock_time = games[0].start_time - timedelta(minutes=5)
        lock_timestamp = int(lock_time.timestamp())

        # Only create if lock time is in the future
        if lock_timestamp <= int(datetime.now(timezone.utc).timestamp()):
            print(f"Lock time already passed for contest {contest_id}")
            return

        print(f"Creating contest {contest_id} with {len(games)} games, lock time: {lock_time}")

        # Build transaction
        nonce = w3.eth.get_transaction_count(owner_account.address)

        tx = contract.functions.createContest(
            contest_id,
            lock_timestamp,
            len(games)
        ).build_transaction({
            'from': owner_account.address,
            'nonce': nonce,
            'gas': 200000,
            'gasPrice': w3.eth.gas_price * 2,
            'chainId': 8453
        })

        # Sign and send
        signed_tx = w3.eth.account.sign_transaction(tx, owner_account.key)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)

        print(f"Contest creation tx sent: {tx_hash.hex()}")

        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt.status == 1:
            print(f"Contest {contest_id} created successfully!")
        else:
            print(f"Contest creation failed for {contest_id}")

    except Exception as e:
        print(f"Error auto-creating contest: {e}")

app = FastAPI(title="PuckPot API", lifespan=lifespan)

# CORS - Allow all origins for public API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
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
    """Poll NHL API for live score updates (checks today and yesterday's contests)"""
    try:
        today = get_et_date(0)
        yesterday = get_et_date(-1)

        # Check both today and yesterday
        dates_to_check = [today, yesterday]

        db = SessionLocal()

        for date_str in dates_to_check:
            contest_id = int(date_str.replace("-", ""))

            # Check if this contest has entries (worth updating)
            entry_count = db.query(EntryDB).filter(EntryDB.contest_id == contest_id).count()
            if entry_count == 0:
                continue

            try:
                # Use schedule endpoint (includes scores) - more reliable than /score for historical dates
                scores = await fetch_nhl_schedule(date_str)
                games = parse_games_from_schedule(scores, date_str)
                print(f"Found {len(games)} games for {date_str}")

                for game in games:
                    # Update or create game result
                    existing = db.query(GameResultDB).filter(
                        GameResultDB.contest_id == contest_id,
                        GameResultDB.nhl_game_id == game.nhl_game_id
                    ).first()

                    if game.status == "FINAL" and game.home_score is not None:
                        winner = 1 if game.home_score > game.away_score else 0

                        if existing:
                            # Only update updated_at when something actually changed
                            if not existing.is_final or existing.home_score != game.home_score or existing.away_score != game.away_score:
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
                    elif game.status == "POSTPONED":
                        # Postponed games count as final (winner=2) so settlement can still fire
                        if not existing:
                            db.add(GameResultDB(
                                contest_id=contest_id,
                                game_index=game.game_index,
                                nhl_game_id=game.nhl_game_id,
                                home_score=0,
                                away_score=0,
                                winner=2,
                                is_final=True
                            ))
                        elif not existing.is_final:
                            existing.winner = 2
                            existing.is_final = True
                            existing.updated_at = datetime.utcnow()

                db.commit()
            except Exception as e:
                print(f"Error updating scores for {date_str}: {e}")

        db.close()
    except Exception as e:
        print(f"Error updating live scores: {e}")

async def check_contests_to_settle():
    """Check for contests that can be settled and auto-settle them (checks today and yesterday)"""
    try:
        today = get_et_date(0)
        yesterday = get_et_date(-1)

        # Check both today and yesterday's contests
        contest_ids_to_check = [
            int(today.replace("-", "")),
            int(yesterday.replace("-", ""))
        ]

        db = SessionLocal()

        for contest_id in contest_ids_to_check:
            contest = db.query(ContestDB).filter(ContestDB.id == contest_id).first()

            if not contest:
                continue

            num_games = len(contest.games) if contest.games else 0
            final_results = db.query(GameResultDB).filter(
                GameResultDB.contest_id == contest_id,
                GameResultDB.is_final == True
            ).all()

            if len(final_results) == num_games and num_games > 0:
                # Check if contract is already settled
                contract_data = await get_contract_data(contest_id)
                if contract_data["state"] == "settled":
                    print(f"Contest {contest_id} already settled on-chain")
                    continue

                print(f"Contest {contest_id} ready for settlement - all {num_games} games final")

                # Auto-settle the contest on-chain
                await auto_settle_contest(contest_id, final_results, db)

        db.close()
    except Exception as e:
        print(f"Error checking contests: {e}")

async def auto_settle_contest(contest_id: int, results: list, db):
    """Automatically settle a contest on the blockchain"""
    if not CONTRACT_ADDRESS or not owner_account:
        print("Cannot auto-settle: CONTRACT_ADDRESS or OWNER_PRIVATE_KEY not configured")
        return

    try:
        # Sort results by game_index and build results array
        results_sorted = sorted(results, key=lambda r: r.game_index)
        results_array = [r.winner for r in results_sorted]

        # Calculate highest scoring game total goals
        highest_total = 0
        for r in results_sorted:
            if r.home_score is not None and r.away_score is not None:
                total = r.home_score + r.away_score
                if total > highest_total:
                    highest_total = total

        print(f"Settling contest {contest_id}")
        print(f"  Results: {results_array}")
        print(f"  Highest scoring game total: {highest_total}")

        # Build and send transaction
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=PUCKPOT_ABI
        )

        # Get nonce
        nonce = w3.eth.get_transaction_count(owner_account.address)

        # Build transaction
        tx = contract.functions.settleContest(
            contest_id,
            results_array,
            highest_total
        ).build_transaction({
            'from': owner_account.address,
            'nonce': nonce,
            'gas': 500000,
            'gasPrice': w3.eth.gas_price * 2,  # Pay 2x for faster confirmation
            'chainId': 8453  # Base mainnet
        })

        # Sign and send
        signed_tx = w3.eth.account.sign_transaction(tx, owner_account.key)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)

        print(f"Settlement tx sent: {tx_hash.hex()}")

        # Wait for confirmation
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt.status == 1:
            print(f"Contest {contest_id} settled successfully! Block: {receipt.blockNumber}")
        else:
            print(f"Settlement transaction failed for contest {contest_id}")

    except Exception as e:
        print(f"Error auto-settling contest {contest_id}: {e}")

# API Endpoints
@app.get("/")
async def root():
    return {"message": "PuckPot API", "version": "1.0.0"}

@app.get("/api/contests/today", response_model=Contest)
async def get_today_contest():
    """Get the active contest.
    Uses Eastern Time for dates (NHL schedule timezone).
    Always shows yesterday's contest while it's unsettled or within 15 min of settlement.
    Only switches to today's games once the previous contest is fully complete."""
    et_today = get_et_date(0)
    et_yesterday = get_et_date(-1)
    active_date = et_today

    try:
        # Step 1: Check if yesterday's ET contest should still be shown.
        # Keep showing it until: settled on-chain AND 15+ min since last game became final.
        yesterday_contest_id = int(et_yesterday.replace("-", ""))
        yesterday_contract_data = await get_contract_data(yesterday_contest_id)

        if yesterday_contract_data.get("entrant_count", 0) > 0:
            if yesterday_contract_data.get("state") != "settled":
                # Has entries but not yet settled — keep showing yesterday UNLESS
                # it's been 10+ hours since lock time (games must be long over).
                # This prevents the app getting stuck if settlement fails on-chain.
                db_tmp = SessionLocal()
                y_contest = db_tmp.query(ContestDB).filter(ContestDB.id == yesterday_contest_id).first()
                db_tmp.close()

                too_old = False
                if y_contest and y_contest.lock_time:
                    lt = y_contest.lock_time
                    if lt.tzinfo is None:
                        lt = lt.replace(tzinfo=timezone.utc)
                    hours_since_lock = (datetime.now(timezone.utc) - lt).total_seconds() / 3600
                    too_old = hours_since_lock >= 10  # 10 h covers even the latest games + buffer

                if not too_old:
                    active_date = et_yesterday
                # else: fall through to today (settlement is overdue)
            # If settled (prize already paid), fall through and show today's new games immediately

        # Step 2: Fetch schedule for the active date
        schedule = await fetch_nhl_schedule(active_date)
        games = parse_games_from_schedule(schedule, active_date)

        # Step 3: If active_date had no games, fall back to the other date
        if not games and active_date == et_yesterday:
            active_date = et_today
            schedule = await fetch_nhl_schedule(active_date)
            games = parse_games_from_schedule(schedule, active_date)

        if not games:
            raise HTTPException(status_code=404, detail="No games scheduled today")

        # Lock time = 5 minutes before first puck drop
        lock_time = games[0].start_time - timedelta(minutes=5)
        contest_id = int(active_date.replace("-", ""))

        # Store/update contest in database
        db = SessionLocal()
        contest_db = db.query(ContestDB).filter(ContestDB.id == contest_id).first()

        games_json = [g.model_dump(mode='json') for g in games]

        if not contest_db:
            contest_db = ContestDB(
                id=contest_id,
                date=active_date,
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
            date=active_date,
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

@app.post("/api/admin/sync-scores")
async def manual_sync_scores():
    """Manually trigger score sync (for debugging/testing)"""
    await update_live_scores()
    await sync_entries_from_blockchain()
    return {"message": "Scores and entries synced"}

@app.post("/api/admin/create-today-contest")
async def manual_create_contest():
    """Manually trigger today's contest creation on-chain"""
    if not CONTRACT_ADDRESS or not owner_account:
        return {
            "success": False,
            "error": "CONTRACT_ADDRESS or OWNER_PRIVATE_KEY not configured in environment",
            "contract_address_set": bool(CONTRACT_ADDRESS),
            "private_key_set": bool(OWNER_PRIVATE_KEY),
        }
    await auto_create_daily_contest()
    return {"success": True, "message": "Contest creation triggered — check logs for result"}

@app.get("/api/admin/status")
async def admin_status():
    """Check backend config and today's on-chain contest status"""
    today = get_et_date(0)
    contest_id = int(today.replace("-", ""))
    contract_data = await get_contract_data(contest_id)
    return {
        "contract_address_set": bool(CONTRACT_ADDRESS),
        "private_key_set": bool(OWNER_PRIVATE_KEY),
        "contract_address": CONTRACT_ADDRESS or None,
        "today_contest_id": contest_id,
        "contest_exists_on_chain": (contract_data.get("lock_time", 0) or 0) > 0,
        "contest_data": contract_data,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
