# SAMgovArby

Government contract arbitrage pipeline. Finds small-cap stock opportunities from federal contract awards.

## ⚠️ TOKEN OPTIMIZATION (RTK) — MANDATORY, NON-NEGOTIABLE

**EVERY bash command MUST have `rtk` prefix. This is not optional.**

**This saves 60-90% tokens. Violation wastes your money and will result in subscription cancellation.**

```bash
rtk python build_training_set.py    # 90%+ savings
rtk python -m pytest tests/          # 99% savings
rtk git status && rtk git diff       # 59-80% savings
rtk ls -la datasets/                 # 65% savings
```

**NO EXCEPTIONS. Use `rtk` even if unsure — it passes through unchanged if no filter exists.**

## Key Files

| File | Purpose |
|------|---------|
| `build_training_set.py` | 3-stage training dataset builder (checkpoint-resumable) |
| `backtest.py` | Replay historical awards through filter → score → simulate |
| `optimizer.py` | Grid-search over threshold/TP/SL/hold/max_mcap; avg_pnl/week ranking |
| `scoring_engine.py` | 5-factor scoring (value-to-mcap, sole-source, first-agency, hot-sector, no-PR) |
| `filter_engine.py` | Live trading filter — rejection criteria, thread-safe mcap cache |
| `filter_engine_bt.py` | Backtest filter — uses pre-computed training CSV; value-to-mcap ratio check |
| `price_sim.py` | Trade simulator — enters at open_t1 (no look-ahead); 0.5% slippage + 0.1% commission |
| `resolver/` | V1 resolver package — DuckDB-backed, bulk EDGAR enrichment, parallelized |
| `cage_resolver.py` | CAGE code → GLEIF/LEI lookup |
| `lei_resolver.py` | LEI → ticker via OpenFIGI/SEC mapping |
| `sam_entity_client.py` | SAM.gov Entity API client (CAGE → canonical legal name) |
| `enrich_ohlc.py` | Standalone OHLC enrichment for training rows |
| `award_cache.py` | Award-level caching layer |
| `build_issuer_master.py` | Build/refresh issuer master table from EDGAR |
| `sam_poller.py` | SAM.gov polling — extracts cage_code; sole-source via `config.is_sole_source()` |
| `edgar_client.py` | EDGAR submissions/8-K/dilutive filings — RateLimiter throttled |
| `news_checker.py` | Google News RSS press-release check — RateLimiter throttled |
| `rate_limiter.py` | Thread-safe `RateLimiter` class used by edgar_client and news_checker |
| `trade_executor.py` | Alpaca bracket orders (GTC); validates response; warns on missing credentials |
| `main.py` | Live pipeline: poll → filter → score → trade; skips below `MIN_TICKER_CONFIDENCE` |
| `bulk_builder.py` | Alternative dataset builder from USASpending API |
| `gui.py` | PyQt6 GUI — process management, logging, backtest/optimizer panels |

## Data Flow

```
USASpending bulk CSV
  → Stage 1 (filter_training_set.csv)   — IDIQ removed, $1M–$10B range
  → Stage 2 (stage2_with_tickers.csv)   — TickerResolverV4 (deduped by entity)
  → Stage 3 (training_set_final.csv)    — OHLC, shares, hist mcap, 8-K, dilutive, PR
  → backtest.py / optimizer.py
```

**Checkpoints:** `datasets/checkpoints/stage{1,2,3}_{filter,tickers,enrich}.json`
- Stage 2 saves every 200 entities; Stage 3 saves every 50 awards

**Caches:** `.ticker_cache_v4.json`, `.mcap_cache.json`, `.edgar_tickers.json`

## USASpending Data Source

Bulk download: `https://files.usaspending.gov/award_data_archive/`
File: `FY{year}_All_Contracts_Full_YYYYMMDD.zip` → extract into `datasets/`

## Constants (`config.py`)

| Constant | Value | Notes |
|----------|-------|-------|
| `MIN_CONTRACT_VALUE` | $1M | Stage 1 + live filter |
| `MAX_MARKET_CAP` | $5B | Wide net — optimizer tunes the real cutoff |
| `SCORE_THRESHOLD` | 30 | Minimum score to place a trade |
| `MIN_TICKER_CONFIDENCE` | `"low"` | Accepts low+ confidence resolutions in backtest |
| `SLIPPAGE_PCT` | 0.5% | Applied per side in price_sim.py |
| `COMMISSION_PCT` | 0.1% | Applied per side in price_sim.py |

**Scoring weights:** value-to-mcap (30) + sole-source (25) + first-agency (15) + hot-sector (15) + no-PR (15) = 100 max

**Sole-source detection** is centralized in `config.is_sole_source()` — checks `extent_competed_code`, `num_offers == 1`, description keywords, and `other_than_full_open`. Used by `sam_poller.py`, `scoring_engine.py`, and `build_training_set.py`.

## Known Pre-Existing Test Failures

Two tests fail on a clean checkout (see `docs/FAILING_TESTS.md`):

- `test_sam_gov_reader.py::test_valid_row_yields_contract_record` — fixture uses `"Period of Performance Start Date"` but reader now reads `"Date Signed"`
- `test_ticker_resolver_v4.py::test_tier1_cage_resolves_to_ticker` — test mocks `cage_resolver` attribute but Tier 1 now calls GLEIF REST API directly

## Notes

- USASpending API: requires User-Agent header; max page size 100 (422 on higher values)
- EDGAR rate limit: 0.12 s/req (hard floor); submissions cached per CIK in-memory
- Resolver V1 (`resolver/`) is the active resolver — DuckDB-backed, replaces TickerResolverV4
- No Sharpe ratio anywhere in the codebase (removed — EDGAR date granularity made it meaningless)
- 8-K timing stored as `days_to_8k` (integer days), not hours
- `price_sim.py` enters at `open_t1` (day after award) — no look-ahead bias
- Optimizer ranks by `avg_pnl_per_week` (N≥5); GUI uses same metric for consistency
- `filter_engine_bt.py` Filter 3b: contract value-to-mcap must be 1%–500%
