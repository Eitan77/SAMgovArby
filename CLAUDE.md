# SAMgovArby

Government contract arbitrage pipeline. Finds small-cap stock opportunities from federal contract awards.

## Token Optimization (RTK)

**ALWAYS prefix bash commands with `rtk`** — saves 60-90% tokens on output.

```bash
# Build & Run (90%+ savings)
rtk python build_training_set.py    # Only errors & progress lines
rtk python -m pytest tests/          # Only test failures (99%)

# Git (59-80% savings)
rtk git status                       # Compact status
rtk git log --oneline -10           # Compact log
rtk git diff                         # Diff with 80% token savings

# File inspection (60-75% savings)
rtk ls -la datasets/                 # Tree format, minimal
rtk grep "ERROR" build_training_set.py  # Grouped by file
```

**RTK golden rule:** Always safe to use, always passes through if no filter exists. Never breaks commands.

## Key Files
- `build_training_set.py` — 3-stage training dataset builder (checkpoint-resumable)
  - Stage 1: Load & Filter — bulk CSV from USASpending, remove IDIQ, top-20, filter $1M–$10B
  - Stage 2: Ticker Resolution — EDGAR fuzzy match via TickerResolverV2
  - Stage 3: Enrich — OHLC prices, shares, historical market cap, 8-K, dilutive filings, agency history
- `backtest.py` — Replay historical awards through filter → score → simulate
- `optimizer.py` — Grid-search over threshold/TP/SL/hold parameters
- `scoring_engine.py` — 5-factor scoring (value-to-mcap, sole-source, first-agency, hot-sector, no-PR)
- `filter_engine.py` — Live trading filter (6 rejection criteria)
- `filter_engine_bt.py` — Backtest filter (uses pre-computed training CSV signals)
- `ticker_resolver.py` — Multi-stage ticker resolution with confidence levels
- `bulk_builder.py` — Alternative dataset builder from USASpending API
- `main.py` — Live trading pipeline (poll → filter → score → trade via Alpaca)

## USASpending Data Source
- Bulk download: `https://files.usaspending.gov/award_data_archive/`
- File: `FY{year}_All_Contracts_Full_YYYYMMDD.zip` → place in `datasets/`
- Contains CSV with all contract awards for the fiscal year

## Data Flow
- Stage outputs: `filtered_training_set.csv` → `stage2_with_tickers.csv` → `training_set_final.csv`
- Checkpoints: `datasets/checkpoints/stage1_filter.json`, `stage2_tickers.json`, `stage3_enrich.json`
- Ticker cache: `.ticker_cache_v2.json`
- EDGAR map: `.edgar_tickers.json`

## Constants
- MIN_CONTRACT_VALUE: $1M
- MAX_AWARD_AMOUNT: $10B
- MAX_MARKET_CAP (config.py): $300M (live/backtest filter)
- Scoring: value-to-mcap (30), sole-source (25), first-agency (15), hot-sector (15), no-PR (15) = 100 max
- SCORE_THRESHOLD: 40

## Speed Optimizations

| Change | File | Impact |
|--------|------|--------|
| Merged old Stage 3+4 into single Stage 3 | `build_training_set.py` | Eliminates redundant current-mcap API calls |
| Shares deduped by ticker (cached) | `build_training_set.py` | 1 yfinance call/unique ticker vs 1/award |
| Shared ThreadPoolExecutor (4 workers) | `build_training_set.py` | No per-award executor creation overhead |
| Batch CP saves every 50 rows | `build_training_set.py` | Reduces disk I/O ~50x |
| `bulk_builder._fuzzy_match_edgar`: use `process.extractOne` | `bulk_builder.py` | C-speed vs Python O(N) loop |

**Remaining bottlenecks:**
- EDGAR submissions (0.12s/req rate limit) — throttle is hard floor, already cached per CIK in-memory
- `_fetch_price_window` — one yfinance download per award/date; batching hard since dates differ
- Data loading: full FY CSV is large; month filter already applied in Stage 1

## Notes
- USASpending API requires User-Agent header; uses requests.Session with HTTPAdapter/Retry for connection pooling
- API max page size is 100 (returns 422 for higher values)
- March 2023 data in `datasets/checkpoints/` if available, else download from USASpending archive
