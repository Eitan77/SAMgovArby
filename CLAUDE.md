# SAMgovArby

Government contract arbitrage pipeline. Finds small-cap stock opportunities from federal contract awards.

## ⚠️ TOKEN OPTIMIZATION (RTK) — MANDATORY, NON-NEGOTIABLE

**EVERY bash command MUST have `rtk` prefix. This is not optional.**

**This saves 60-90% tokens. Violation wastes your money and will result in subscription cancellation.**

```bash
# ALL commands must use rtk:
rtk python build_training_set.py    # 90%+ savings
rtk python -m pytest tests/          # 99% savings
rtk git status                       # 59-80% savings
rtk git log --oneline -10           # Compact log
rtk git diff                         # 80% savings
rtk ls -la datasets/                 # 65% savings
rtk grep "ERROR" file.py             # 75% savings
rtk find . -name "*.csv"             # 70% savings
rtk curl https://example.com         # 70% savings
```

**NO EXCEPTIONS. Use `rtk` even if unsure.**
- It's always safe—passes through unchanged if no filter exists
- Never breaks commands
- Always saves tokens or at worst does nothing

**Savings breakdown:**
- Tests (pytest/vitest/playwright): 90-99%
- Build (cargo/tsc/next/prettier): 70-87%
- Git (status/log/diff/add/commit/push): 59-80%
- Files (ls/grep/find): 60-75%
- Network (curl/wget): 65-70%

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
