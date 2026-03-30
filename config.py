import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
SAM_API_KEY = os.getenv("SAM_API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# Filter thresholds
MAX_MARKET_CAP = 5_000_000_000  # $5B (wide net — optimizer tunes the real cutoff)
MIN_CONTRACT_VALUE = 1_000_000  # $1M
MAX_AWARD_AMOUNT = 10_000_000_000  # $10B hard ceiling (skip M&O mega-contracts)

# Dataset builder
TOP_N_TO_REMOVE = 20  # remove top-N companies by contract count in Stage 1

# Scoring weights (sum = 100)
SCORE_WEIGHTS = {
    "value_to_mcap": 30,    # contract value as % of market cap
    "sole_source":   25,    # sole-source contract
    "first_agency":  15,    # first-time win from this agency
    "hot_sector":    15,    # NAICS in hot sector
    "no_pr":         15,    # no simultaneous press release
}
SCORE_THRESHOLD = 40

# Bracket order params
TAKE_PROFIT_PCT = 0.08  # +8%
STOP_LOSS_PCT = 0.07    # -7%
POSITION_SIZE = 200     # $ per trade
MAX_HOLD_DAYS = 4       # trading days

# Timezone
TZ = "US/Eastern"

# EDGAR rate limit (seconds between requests — SEC limit is 10 req/s)
EDGAR_RATE_LIMIT = 0.12
EDGAR_USER_AGENT = os.getenv("EDGAR_USER_AGENT", "SAMgovArby research@example.com")

# EDGAR enrichment window: look for 8-K within N days AFTER the contract award
# (distinct from MAX_8K_WINDOW_DAYS which is the filter rejection window)
EDGAR_8K_ENRICHMENT_DAYS = 30

# Hot sectors (NAICS prefixes)
HOT_SECTOR_NAICS = {
    "336411", "336414", "336415", "336419",  # aerospace/defense mfg
    "334511", "334519",  # navigation/detection instruments
    "541715",  # R&D physical/bio/engineering
    "518210",  # data processing / AI
    "336413",  # guided missiles
    "927110",  # national security
}
GENERAL_DEFENSE_NAICS_PREFIX = "33641"

# Polling interval
POLL_INTERVAL_HOURS = 1

# ─── Tunable backtest thresholds (used by filter_engine_bt) ──────────────────
MAX_8K_WINDOW_DAYS = 2          # reject if 8-K filed within N days of award
MAX_DILUTIVE_WINDOW_DAYS = 60   # reject if S-1/S-3 within N days before award
MAX_PR_WINDOW_DAYS = 2          # PR within N days counts as "already public"
MIN_TICKER_CONFIDENCE = "medium"   # minimum resolver confidence to accept
