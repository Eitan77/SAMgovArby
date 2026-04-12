import os
from dotenv import load_dotenv

load_dotenv()


def user_cache_dir() -> str:
    """Return (and create) the user-level cache dir for shared API/reference data.

    Stored at ~/.cache/samgovarby/ so it survives git clean / code resets.
    Universal data (EDGAR map, market caps, CAGE→LEI, SAM entity) lives here.
    Per-dataset data (.ticker_cache_v4.json) stays in the project directory.
    """
    d = os.path.join(os.path.expanduser("~"), ".cache", "samgovarby")
    os.makedirs(d, exist_ok=True)
    return d

# API Keys
SAM_API_KEY = os.getenv("SAM_API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# Filter thresholds
MAX_MARKET_CAP = 5_000_000_000  # $5B (wide net — optimizer tunes the real cutoff)
MIN_CONTRACT_VALUE = 1_000_000  # $1M
MAX_AWARD_AMOUNT = 10_000_000_000  # $10B hard ceiling (skip M&O mega-contracts)

# Value-to-market-cap ratio filter (contract as % of company's market cap)
# Below MIN: contract too immaterial to move the stock
# Above MAX: zombie/penny stocks with near-zero market cap (contract >> mcap)
# NOTE: Intentionally wide — high ratios (20-300%) are GOOD signals for small-caps.
# Scoring already penalizes low ratios. Filter is only a sanity-check for extremes.
MIN_VALUE_TO_MCAP_PCT = 0.01   # 1% minimum — filter only truly immaterial contracts
MAX_VALUE_TO_MCAP_PCT = 5.00   # 500% maximum — filter zombie stocks (contract >> mcap)

# Scoring weights (sum = 100)
SCORE_WEIGHTS = {
    "value_to_mcap": 30,    # contract value as % of market cap
    "sole_source":   25,    # sole-source contract
    "first_agency":  15,    # first-time win from this agency
    "hot_sector":    15,    # NAICS in hot sector
    "no_pr":         15,    # no simultaneous press release
}
SCORE_THRESHOLD = 30

# ─── SAM.gov API Configuration ──────────────────────────────────────────────────

SAM_GOV_API_BASE = "https://api.sam.gov/contract-awards/v1/"
SAM_GOV_CONTRACT_ENDPOINT = "search"
SAM_GOV_RECORDS_PER_PAGE = 50  # Reduced from 100 to reduce rate limit hits
SAM_GOV_RATE_LIMIT_SEC = 3.0  # Conservative: 1 request per 3 seconds (very strict rate limiting)
SAM_GOV_RETRY_ATTEMPTS = 5  # More retry attempts for rate limiting
SAM_GOV_RETRY_BACKOFF_FACTOR = 2.0
SAM_GOV_TIMEOUT_SEC = 30  # HTTP request timeout
SAM_GOV_API_KEY = "SAM-178836eb-f9ad-4c50-9872-dc258dba2521"  # WARN: Do not commit this to git

# EOD exit params (primary)
TP_PCT = 0.04           # 4% take profit — checked at end-of-day close
SL_PCT = 0.025          # 2.5% stop loss — checked at end-of-day close
MAX_HOLD_DAYS = 3       # trading days before time exit

# Ratchet (trailing stop) params — kept for backwards compat with old backtest runs
GAP_PCT = 0.02          # 2% trailing gap — stop = entry + peak_gain - gap
POSITION_SIZE = 200     # $ per trade

# Backtest realism: slippage + commission
SLIPPAGE_PCT = 0.005    # 0.5% adverse fill per side
COMMISSION_PCT = 0.001  # 0.1% commission per side (0.2% round-trip)

# Timezone
TZ = "US/Eastern"

# EDGAR rate limit (seconds between requests — SEC limit is 10 req/s)
EDGAR_RATE_LIMIT = 0.12
EDGAR_USER_AGENT = os.getenv("EDGAR_USER_AGENT", "SAMgovArby research@example.com")

# EDGAR enrichment window: look for 8-K within N days AFTER the contract award
# (distinct from MAX_8K_WINDOW_DAYS which is the filter rejection window)
EDGAR_8K_ENRICHMENT_DAYS = 30

# GLEIF API (free, no auth)
GLEIF_SEARCH_URL = "https://leilookup.gleif.org/api/v3/lei-records"

# OpenFIGI API (free, no daily limits)
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# Cache TTLs (days)
LEI_CACHE_TTL = 30
TICKER_CACHE_TTL = 7
CAGE_CACHE_TTL = 30

# ─── Sole-source detection (single source of truth) ─────────────────────────

SOLE_SOURCE_CODES = {"B", "C", "G", "CDO", "URG", "SP2"}
SOLE_SOURCE_INDICATORS = {"sole source", "only one source", "one responsible source", "unique source"}


def is_sole_source(extent_competed_code: str = "", description: str = "",
                   num_offers: str = "", other_than_full_open: str = "") -> bool:
    """Single source of truth for sole-source determination."""
    if extent_competed_code.upper() in SOLE_SOURCE_CODES:
        return True
    if str(num_offers).strip() == "1":
        return True
    desc_lower = description.lower()
    if any(ind in desc_lower for ind in SOLE_SOURCE_INDICATORS):
        return True
    otfo = (other_than_full_open or "").strip().upper()
    if otfo and otfo not in ("", "NO", "N"):
        return True
    return False


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
MIN_TICKER_CONFIDENCE = "low"      # minimum resolver confidence to accept
