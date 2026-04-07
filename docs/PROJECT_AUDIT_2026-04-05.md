# SAMgovArby Comprehensive Project Audit

**Date:** 2026-04-05
**Scope:** Full technical, architectural, and conceptual review of the entire pipeline

---

## Executive Summary

This audit covers every subsystem: training pipeline, ticker resolution, scoring/filtering, backtest/optimizer, live trading, and GUI. The project has a solid conceptual foundation — finding small-cap stock alpha from federal contract awards — but has significant gaps that would prevent reliable live trading.

**Key findings:**
- **Live trading is not production-ready.** Backtest and live paths use different filters, different scoring ceilings (100 vs ~70), and different data sources. Results from backtesting do not predict live performance.
- **Ticker resolution** has multiple version fragmentation (V2/V3/V4) with inconsistent thresholds, uncalibrated confidence levels, and critical validation gaps.
- **Backtest statistics are flawed.** Invalid Sharpe ratio formula, no slippage/commission modeling, and look-ahead bias in OHLC data.
- **Security:** SAM.gov API key is hardcoded and committed to git history.
- **GUI** is suitable for backtesting but lacks real-time trading features (live P&L, order status, alerts).

---

## Table of Contents

1. [Security Issues](#1-security-issues)
2. [Training Pipeline](#2-training-pipeline)
3. [Ticker Resolution System](#3-ticker-resolution-system)
4. [Scoring Engine](#4-scoring-engine)
5. [Filter Engines (Live vs Backtest)](#5-filter-engines)
6. [Backtest & Optimizer](#6-backtest--optimizer)
7. [Live Trading Pipeline](#7-live-trading-pipeline)
8. [GUI](#8-gui)
9. [Cross-Cutting Issues](#9-cross-cutting-issues)
10. [Conceptual & Strategic Issues](#10-conceptual--strategic-issues)
11. [Priority Fix Roadmap](#11-priority-fix-roadmap)

---

## 1. Security Issues

### 1.1 CRITICAL: Hardcoded API Key in Source Code
- **File:** `config.py:36`
- SAM.gov API key is hardcoded: `SAM_GOV_API_KEY = "SAM-178836eb-..."` with a comment saying "do not commit to git" — but it IS committed.
- Also appears in `docs/PIPELINE_BUGS_ANALYSIS.md`.
- **Fix:** Move to `.env`, regenerate key, scrub from git history with `git filter-repo`.

### 1.2 Dead Config Variable
- **File:** `config.py:7`
- `SAM_API_KEY = os.getenv("SAM_API_KEY")` is defined but never imported anywhere. The actual variable used is `SAM_GOV_API_KEY` (hardcoded). Developers setting `SAM_API_KEY` env var will see it silently ignored.

### 1.3 Alpaca Credentials Not Validated at Startup
- **File:** `trade_executor.py:30`
- Alpaca client is created lazily on first trade. Missing credentials won't be caught until real money is on the line.

---

## 2. Training Pipeline

### 2.1 CRITICAL: Date Format Mismatch
- **Files:** `sam_gov_reader.py:128` returns `YYYY-MM-DD`; `build_training_set.py:791-793` assumes `M/D/YYYY`.
- Stage 3 splits on `/` to normalize dates. A date like `"2023-03-15"` yields 1 part (not 3), so the normalization silently fails, and `normalized_date` stays `None`. Price fetching and historical shares are skipped for ALL awards with properly formatted dates.
- **Impact:** Core functionality broken — no OHLC data enriched for awards with ISO dates.

### 2.2 CRITICAL: EDGAR Throttle Not Thread-Safe
- **File:** `build_training_set.py:94-102`
- Global `_edgar_last` is read/written by multiple threads (Stage 3 uses `ThreadPoolExecutor(max_workers=8)`). Two threads can read the same elapsed time, both decide to proceed, and both hit SEC simultaneously — violating rate limits and risking IP blocks.
- **Fix:** Wrap in `threading.Lock()`.

### 2.3 Failed EDGAR Responses Cached Permanently
- **File:** `build_training_set.py:894`
- If `_fetch_edgar_submissions()` fails (SEC temporarily down), it returns `{}` which is cached. The same CIK will never be retried until checkpoints are manually cleared.
- **Fix:** Don't cache failures, or cache with TTL.

### 2.4 Stage 2 Resume Skips New Awards for Known Entities
- **File:** `build_training_set.py:438-457`
- When resuming, if Award A (Company X) was already resolved and checkpointed, Award B (also Company X, same entity key) is skipped entirely. New awards from previously-resolved companies won't get tickers on resume.

### 2.5 Historical Shares Fallback Bug
- **File:** `build_training_set.py:877-885`
- When `ticker` is empty string and `date_str` is missing, the `else` branch does `shares_cache.get("", 0)` and hardcodes source as `"split_adjusted"` — incorrect labeling for what is actually missing data.

### 2.6 Dilutive Filing Window Direction Ambiguous
- **File:** `build_training_set.py:733-747`
- `_find_last_dilutive_before_date()` looks BACKWARD (180 days before award). But if the intent is to reject awards where dilution happens AFTER the award, the logic is inverted. The function name says "before_date" but the scoring interpretation may expect "after_date."

### 2.7 8-K Hours Column Misleading
- **File:** `build_training_set.py:714`
- `hours_to_8k` is calculated as `days * 24`. EDGAR only stores dates (not times), so values are always multiples of 24. Column name implies hour-level precision that doesn't exist.

### 2.8 Sole-Source Detection Duplicated in 3 Places
- `build_training_set.py:207-210` (old CSV format)
- `build_training_set.py:238-242` (ContractRecord format)
- `ticker_resolver_v4.py:46` (extent competed codes)
- Each uses different logic. No single source of truth.

### 2.9 Agency History Type Inconsistency
- **File:** `build_training_set.py:340` returns `int`; line 970 defaults to `""`. CSV consumers can't distinguish "0 wins" from "data missing."

### 2.10 Contract Value Filter Ignores Market Cap Context
- `MIN_CONTRACT_VALUE = $1M` is an absolute threshold. A $1M contract to a $50B company is noise; a $500K contract to a $50M company is transformative. Ratio-based filtering should happen in Stage 3 but doesn't.

---

## 3. Ticker Resolution System

### 3.1 CRITICAL: Version Fragmentation (V2/V3/V4)
- Three incompatible versions exist: `ticker_resolver.py` (V2), `ticker_resolver_v3.py`, `ticker_resolver_v4.py`.
- Different APIs: V2/V3 accept bare strings; V4 accepts `ContractRecord` objects.
- Different caching: `.ticker_cache_v2.json` vs `.ticker_cache_v4.json` — no cross-version reuse.
- Different thresholds: V2/V3 use fuzzy threshold 80/85, V4 uses 70/75 (lowered without analysis).
- **Fix:** Consolidate to single version, migrate caches.

### 3.2 CRITICAL: Missing Market Cap Validation in CAGE Tier (V4)
- **File:** `ticker_resolver_v4.py:268-275`
- When resolving via CAGE -> GLEIF -> LEI -> OpenFIGI (highest confidence tier), the code fetches market cap but NEVER checks `if mc > 0` before returning. All other tiers validate this. Could return private companies or companies with zero market cap.

### 3.3 CRITICAL: Empty String After Suffix Stripping
- **File:** `ticker_resolver_v3.py:95-101`
- `_strip_suffixes()` can strip ALL words if the company name is entirely common suffixes (e.g., "GLOBAL USA INC" -> ""). This empty string becomes a cache key, causing silent false negatives.
- **Test cases:** "TECHNOLOGIES INCORPORATED CORP" -> "", "COMPANY LP DE" -> "", "GLOBAL USA INC" -> "".

### 3.4 Normalization Strips Meaningful Punctuation
- **File:** All versions, `_normalize()` regex `[^A-Z0-9 ]`
- "AT&T" -> "ATT", "WL GORE & ASSOCIATES" -> "WL GORE ASSOCIATES", "URS E&C / RAYTHEON" -> "URS EC RAYTHEON". Two companies with different punctuation could normalize to the same string.

### 3.5 Confidence Levels Not Calibrated
- "high" confidence is assigned to EDGAR exact match without SEC submission validation.
- "high" confidence for fuzzy match >= 95% — but fuzzy matches can still be wrong ("SMITH BROTHERS" matching "SMITH CORP").
- No backtesting of precision/recall per confidence level against ground truth.
- `MIN_TICKER_CONFIDENCE = "medium"` in config — but what does "medium" actually represent in accuracy terms?

### 3.6 Confidence Level Naming Inconsistent
- Some paths return `"high"`, `"medium_high"`, `"low_medium"` (snake_case variants).
- No enum or constant set; string comparisons are fragile.

### 3.7 Substring Match Threshold Inconsistency
- V2/V3: min 10 chars, 60% overlap required.
- V4: min 7 chars, 50% overlap. More permissive — higher false positive risk.

### 3.8 CAGE Resolver Doesn't Validate GLEIF Response Structure
- **File:** `cage_resolver.py:85-98`
- Accesses `records[0].get("lei")` without verifying key exists. If GLEIF API changes structure, fails silently.

### 3.9 LEI Resolver Confidence Boosting Bug
- **File:** `lei_resolver.py:69-74`
- Boosts confidence by +0.1 when OpenFIGI succeeds, BEFORE GLEIF validation. Result: confidence can be 1.0 even when GLEIF validation failed.

### 3.10 EDGAR Map Staleness (7-Day Cache)
- IPOs and delistings during the 7-day window are missed. EDGAR data changes weekly.

### 3.11 No Validation That Tickers Are Valid Format
- No regex check that resolved tickers are 1-5 uppercase letters. Could accept "INVALID_TICKER" or "NONE" from APIs.

### 3.12 Cache Race Conditions
- **File:** `api_cache.py:35-41`
- JSON cache writes have no file locking or atomic rename. Multiple processes can corrupt the cache.

### 3.13 GLEIF API Calls Not Rate-Limited
- `cage_resolver.py` and `lei_resolver.py` make multiple GLEIF requests per company with no rate limiting. Bulk resolution of 10K companies could get IP-blocked.

### 3.14 Single-Word Companies Miss Substring Match
- Substring candidates require >= 2 words. Companies like "APPLE" or "3M" can never be caught via substring match.

### 3.15 No Monitoring of Resolution Quality
- No tracking of success rate by tier, confidence distribution, or false positive rate. Quality degradation goes undetected.

---

## 4. Scoring Engine

### 4.1 CRITICAL: Sole-Source Type Validation Missing
- **File:** `scoring_engine.py:45`
- `if contract.get("sole_source")` checks truthiness. String `"False"` is truthy in Python -> awards 25 points incorrectly. `backtest.py:274` correctly validates strings, but live path doesn't.

### 4.2 CRITICAL: Live Scoring Ceiling is ~70, Not 100
- `no_pr` factor (15 pts): Live filter makes PR a hard reject. If PR was found, contract is already rejected. If not found, scoring conservatively gives 0 pts (never 15).
- `first_agency` factor (15 pts): Live mode has no agency history data. Always scores 0.
- **Impact:** Live effective max score is ~70/100. With `SCORE_THRESHOLD = 40`, live requires 57% of effective max vs backtest's 40%. Live is systematically more restrictive.

### 4.3 Hot Sector Rounding Error
- **File:** `scoring_engine.py:66`
- `int(15 * 0.53) = int(7.95) = 7`, but comment says "8 of 15". Use `round()` instead of `int()`.

### 4.4 Agency Empty String Bug
- **File:** `scoring_engine.py:52-56`
- If `agency = ""` (missing), then `"" not in agency_history` is always True -> awards full 15 points for missing data.

### 4.5 Value-to-Mcap Thresholds Not Evidence-Based
- **File:** `scoring_engine.py:29-38`
- Tier breakpoints (10%, 5%, 2%, 1%) are not documented as calibrated against historical data. Should validate these correlate with actual trading profits.

### 4.6 No Config Validation
- No assertion that weights sum to 100. No bounds checking on threshold. A typo in config can silently break scoring.

---

## 5. Filter Engines

### 5.1 CRITICAL: Live vs Backtest Filters Are Inconsistent
- **Live (`filter_engine.py`):** 6 filters, real-time API calls for 8-K, press release, dilutive offerings, CURRENT market cap.
- **Backtest CSV (`filter_engine_bt.py:47-126`):** Uses date-based windows from training CSV, HISTORICAL market cap. Missing IDIQ check.
- **Backtest generic (`filter_engine_bt.py:140-193`):** Skips 8-K, PR, and dilutive checks entirely.
- **Impact:** Contracts that pass backtest may fail live and vice versa. Backtest accuracy != live performance.

### 5.2 Two Separate Backtest Filter Functions
- `apply_filters_bt_from_training` (training CSV mode, 6 filters) and `apply_filters_bt` (generic mode, 3 filters) have different logic. Only the training CSV mode is used by `backtest.py`. The generic mode is dead code.

### 5.3 Ticker Confidence Not Filtered in Live Mode
- **File:** `main.py:127-142`
- Live pipeline accepts any confidence level from ticker resolution. Backtest filters on `MIN_TICKER_CONFIDENCE = "medium"`. Low-confidence tickers get traded live but would be filtered in backtest.

### 5.4 Filter Window Parameters Not Justified
- `MAX_8K_WINDOW_DAYS = 2`, `MAX_PR_WINDOW_DAYS = 2`, `MAX_DILUTIVE_WINDOW_DAYS = 60` — no documentation on why these values. Are they evidence-based?

### 5.5 Market Cap Used at Live Time, Not Award Time
- **File:** `filter_engine.py:67-90`
- Live mode fetches TODAY's market cap via yfinance. A company that was $50M at award time but $500M today would score very differently.

---

## 6. Backtest & Optimizer

### 6.1 CRITICAL: Invalid Sharpe Ratio Calculation
- **File:** `backtest.py:375-382`
- Uses `mean / std * sqrt(252 / len(pnls))` — this is not a valid Sharpe ratio. Correct formula: `(mean - risk_free) / std * sqrt(252)`. The current formula penalizes strategies with more trades (more trades = lower Sharpe), which is inverted logic.

### 6.2 CRITICAL: No Slippage, Commission, or Execution Modeling
- All entry/exit prices are exact OHLC values. Real small-cap trading has 0.5-2% slippage and 0.05-0.20% round-trip commissions. Results are systematically optimistic.

### 6.3 CRITICAL: Optimizer Ranks by Total Return, Not Quality
- **File:** `optimizer.py:259-268`
- `_rank_score()` returns total P&L percentage. A strategy with 100 trades x 0.5% avg (50% total) ranks above 5 trades x 12% avg (60% total). Should rank by expectancy, Sharpe, or profit factor.

### 6.4 CRITICAL: OHLC Column Semantics Undefined
- Code uses `open_t0`, `high_t1`, `close_t3` etc. without defining what `tN` means. Is `t0` the award date or the next trading day? Entry/exit correctness depends on this.

### 6.5 CRITICAL: No Seed Control / Reproducibility
- yfinance data can change between runs (dividend adjustments, splits, API version). No snapshot or versioning mechanism. Backtests are not reproducible.

### 6.6 Look-Ahead Bias in OHLC Data
- **File:** `price_sim.py:197-287`
- OHLC data is enriched offline (at training-build time). Entry price `open_t0` was computed after the fact, not during the backtest window. This is a form of look-ahead bias.

### 6.7 Stop Loss Priority Over Take Profit
- **File:** `price_sim.py:101-106`
- SL is checked before TP on each day. If both are hit intraday (volatile stock), SL always wins. This overstates losses on volatile days.

### 6.8 Max Drawdown Not Tied to Account Size
- `POSITION_SIZE = $200` is defined but never used in backtest calculations. Drawdown is reported as percentage but not relative to any account size.

### 6.9 No Gap Risk Modeling
- If a stock gaps down past SL overnight, code assumes SL fills at SL price. Real execution fills at gap-open price (potentially much worse).

### 6.10 No Liquidity Checks
- Backtest trades any ticker that passes filters. Small-caps may have wide bid-ask spreads and low volume. No volume or liquidity requirements.

### 6.11 Optimizer Cache Mode Missing Market Cap Optimization
- **File:** `optimizer.py:54-120`
- `optimize_from_cache()` doesn't loop over `max_market_cap` values. `optimize_from_training_csv()` does. Cache mode misses 6x combos.

### 6.12 Price Data Availability Bias
- **File:** `backtest.py:342`
- If `simulate_trade_from_row()` returns None (missing OHLC), trade is skipped. But delisted or penny stocks are most likely to have missing data AND to be losses. This censors the worst outcomes.

### 6.13 Global Config Mutation
- **File:** `backtest.py:159-175`
- Mutates `config_module.MAX_MARKET_CAP` at runtime. Thread-unsafe and fragile for testing.

### 6.14 Dedup Strategy Inconsistent
- **File:** `backtest.py:221-232`
- Deduplication by `(ticker, award_date[:10])` clears trade fields but retains scoring fields. CSV output has confusing "duplicate" rows with scores but no trades.

---

## 7. Live Trading Pipeline

### 7.1 CRITICAL: Dual Data Sources with Incompatible Schemas
- `main.py` imports from `sam_poller.py` (Opportunities v2 API). `sam_gov_contracts.py` (Contract Awards v1 API) also exists but uses different field names (`naics_code` vs `naics`, `awarding_agency_name` vs `agency`). The second source is dead code but a maintenance liability.

### 7.2 CRITICAL: Missing `cage_code` from Live Pipeline
- **File:** `main.py:129`
- `sam_poller.py` does NOT include `cage_code` in returned data. Ticker resolver's highest-confidence CAGE tier is always skipped in live mode, reducing resolution success.

### 7.3 CRITICAL: Race Condition in Duplicate Position Guard
- **File:** `trade_executor.py:38-55`
- TOCTOU race: between position check and order submission, another process could submit an order for the same ticker. Two positions in the same stock from parallel runs.

### 7.4 CRITICAL: No Transaction Atomicity for Position Tracking
- Three separate steps: (1) place Alpaca order, (2) record to positions.csv, (3) mark award processed. Crash between steps leaves inconsistent state — real money at risk with no local record.

### 7.5 CRITICAL: Bracket Order `time_in_force="day"` Conflicts with Multi-Day Hold
- **File:** `trade_executor.py:95`
- `time_in_force="day"` means position must exit today. But `MAX_HOLD_DAYS=4` suggests multi-day holds. Position gets liquidated at market close, not at TP/SL over 4 days.
- **Fix:** Change to `time_in_force="gtc"`.

### 7.6 CRITICAL: Position Exit Uses Stale Price
- **File:** `trade_executor.py:169-177`
- Force-exit for expired positions uses `alpaca_pos.current_price` (potentially stale) instead of querying actual fill price after order submission.

### 7.7 CRITICAL: Processed Awards Dedup Key Collision
- **File:** `main.py:171-182`
- Key = `[awardee_name, award_amount, posted_date]`. Two different contracts to the same company for the same amount on the same day (different agencies) would collide. Should use PIID if available.

### 7.8 Single-Threaded Scheduler Drift
- **File:** `main.py:229-235`
- `schedule` library is single-threaded. If `run_pipeline()` takes longer than `POLL_INTERVAL_HOURS`, scheduling drifts. A 70-minute pipeline with 60-minute interval runs every 70 minutes instead.

### 7.9 No Process Supervision or Alerting
- If `run_pipeline()` raises an uncaught exception, the process dies silently. No supervisor, no restart, no alert.

### 7.10 No Validation of Alpaca Order Response
- **File:** `trade_executor.py:90-99`
- After `api.submit_order()`, code assumes `order.id` exists and order was accepted. Doesn't check for rejection status.

### 7.11 Positions CSV Write Not Atomic
- **File:** `trade_executor.py:236-241`
- Entire CSV rewritten on every position close. No atomic temp+rename pattern. Process crash during write corrupts the file.

### 7.12 No Pipeline Timeout
- If any API call hangs (SAM.gov, EDGAR, yfinance, Alpaca), the entire pipeline blocks indefinitely.

---

## 8. GUI

### 8.1 CRITICAL: No Real-Time Position P&L
- Positions table shows ticker, entry_date, entry_price, qty — but NO current price or unrealized P&L. User cannot see how much money is at risk. GUI reads `positions.csv` as-is without connecting to Alpaca for live data.

### 8.2 CRITICAL: No Order Status Monitoring
- Cannot see if orders are pending, filled, or rejected. No "cancel order" button. No detection of fills or partial fills.

### 8.3 CRITICAL: Dashboard is Static, Not Real-Time
- Summary stats load once on tab open, never update. New trades appear in signal log but summary cards don't recalculate. User sees stale metrics during market hours.

### 8.4 CRITICAL: Live Pipeline Tab Shows Logs, Not Trading State
- Shows raw `pipeline.log` output ("Processing 42 awards"), not actionable trading information (open positions, P&L, order status). Unsuitable for live trading decisions.

### 8.5 File Watcher Unreliable on Windows
- **File:** `gui.py:486-516`
- `QFileSystemWatcher.fileChanged` is unreliable on Windows (atomic file replacement drops watches). Code re-adds on dispatch but has a race condition.

### 8.6 No Alert/Notification System
- No sound, email, or system notification for trade executions, stop-loss hits, pipeline errors, or market hours. User must watch GUI constantly.

### 8.7 Config Parsing via Regex is Fragile
- **File:** `gui.py:1865-1907`
- Reads/writes Python config.py via regex. No validation that written config is valid Python. Corruption possible.

### 8.8 All Heavy Operations Block GUI
- CSV loading, backtest running, optimizer — all block the event loop. Large CSV files cause UI freezes.

### 8.9 Process Exit Codes Ignored
- **File:** `gui.py:872`
- `_on_finished()` doesn't check exit code. Failed backtests/optimizers show no error indication.

### 8.10 No Error Logging in Exception Handlers
- **File:** `gui.py:304, 429, 471, 506`
- Multiple `except Exception: pass` blocks silently swallow errors. Makes debugging production issues impossible.

---

## 9. Cross-Cutting Issues

### 9.1 Duplicate Rate Limiting Implementations
- Three separate `_rate_limit()` / `_edgar_throttle()` implementations: `edgar_client.py:18-23`, `news_checker.py:14-19`, `ticker_resolver_v3.py`.
- **Fix:** Shared `rate_limiter.py` module.

### 9.2 Inconsistent Logging Configuration
- `main.py` uses `logging.basicConfig()`. `backtest.py` imports `setup_logging()` from `config_logging`. Other scripts use `getLogger(__name__)`. Logging format varies by entry point.

### 9.3 Global Mutable State Without Thread Safety
- `filter_engine.py:13` — `_mcap_cache: dict = {}` (no locking)
- `news_checker.py:11` — `_last_request = 0.0`
- `edgar_client.py:15` — `_last_request = 0`
- All unsafe under threading.

### 9.4 No Input Validation or Schema Validation
- Contract records from APIs used directly without Pydantic models or dataclass validation. Missing fields cause silent failures.

### 9.5 Incomplete Type Hints
- Most modules lack return types and parameter annotations. Makes IDE support poor and bugs harder to catch.

### 9.6 No Test Coverage for Core Pipeline
- Tests exist for: api_cache, cage_resolver, lei_resolver, sam_gov_reader, ticker_resolver_v3/v4.
- **No tests for:** filter_engine, scoring_engine, backtest, optimizer, main, trade_executor.

### 9.7 Dependencies Not Fully Pinned
- `requirements.txt` uses minimum versions (`>=2.31`). No lock file.
- `pandas_market_calendars` imported in `trade_executor.py:197` but not in `requirements.txt`.
- `PyQt6` and `matplotlib` (GUI deps) not in `requirements.txt`.

### 9.8 Timezone Handling Inconsistent
- Some timestamps are naive, some aware. Some use UTC, some US/Eastern.
- `api_cache.py:65` uses `datetime.utcnow()` (naive). `main.py:46-47` creates aware datetime.

### 9.9 Empty String vs None Used Interchangeably
- Throughout codebase, `None` and `""` are used interchangeably. Conditions like `if not ticker` are ambiguous.

### 9.10 `MAX_MARKET_CAP` in config.py Unused
- **File:** `config.py:13`
- `MAX_MARKET_CAP = 5_000_000_000` is defined but never used in any filter or scoring path. Either implement or remove.

### 9.11 EDGAR Submission URL Duplicated
- `edgar_client.py:13` and `ticker_resolver_v3.py:73` both define the same `SUBMISSIONS_URL`. Should be in `config.py`.

---

## 10. Conceptual & Strategic Issues

### 10.1 Backtest Does Not Predict Live Performance
This is the single biggest issue. The backtest and live paths diverge in at least 6 ways:

| Dimension | Backtest | Live |
|-----------|----------|------|
| Market Cap | Historical (at award time) | Current (today) |
| Ticker Confidence | Filtered at "medium" | Not filtered |
| 8-K / PR / Dilutive | Date-window from CSV | Real-time API call |
| Agency History | From training data | Not available (0 pts) |
| No-PR Factor | From CSV enrichment | Hard reject (0 pts) |
| Max Score | 100 | ~70 |
| Effective Threshold | 40% of 100 | 57% of 70 |

**This means:** You cannot use backtest results to predict live trading profitability. They are testing different strategies.

### 10.2 Scoring Weights Are Not Validated
The 5-factor scoring (value-to-mcap 30, sole-source 25, first-agency 15, hot-sector 15, no-PR 15) has no evidence basis. These weights should be calibrated against actual trading outcomes. The optimizer tunes TP/SL/threshold but never tunes scoring weights.

### 10.3 Training Data May Have Survivorship Bias
- Only companies that could be resolved to tickers are backtested. Companies hard to match (name changes, subsidiaries, mergers) are excluded.
- Only companies with yfinance price data are traded. Delisted or illiquid companies are skipped (missing = 0 P&L, not loss).

### 10.4 No Position Sizing or Risk Management
- Fixed $200 position size regardless of account size, conviction level, or market conditions.
- No max concurrent positions limit.
- No daily/weekly loss limit.
- No correlation check (trading 3 defense stocks simultaneously = concentrated risk).

### 10.5 NAICS Hot Sector Check Too Narrow
- **File:** `scoring_engine.py:63-66`
- Exact match on NAICS codes. NAICS is hierarchical — 336419 (guided missile parts) doesn't match 336411 (aircraft engines) even though both are defense manufacturing. Should use prefix matching.

### 10.6 Press Release Scoring Assumes PRs Eliminate All Alpha
- If a company issues a PR about the contract, scoring gives 0 pts on no-PR factor. But PR-announced contracts can still move stocks (especially small-caps with limited analyst coverage).

---

## 11. Priority Fix Roadmap

### P0 — Must Fix Before Live Trading

| # | Issue | Section | Effort |
|---|-------|---------|--------|
| 1 | Remove hardcoded API key, scrub git history | 1.1 | Low |
| 2 | Fix `time_in_force="day"` -> `"gtc"` for bracket orders | 7.5 | Trivial |
| 3 | Align live and backtest filters (single filter path) | 5.1, 10.1 | High |
| 4 | Add sole-source type validation in live scoring | 4.1 | Low |
| 5 | Fix live scoring ceiling (pass PR/agency data or adjust threshold) | 4.2, 10.1 | Medium |
| 6 | Add ticker confidence filtering in live mode | 5.3 | Low |
| 7 | Validate Alpaca order response before recording position | 7.10 | Low |
| 8 | Add atomic position tracking (order + record + mark in transaction) | 7.4 | Medium |
| 9 | Add slippage + commission to backtest | 6.2 | Medium |
| 10 | Fix Sharpe ratio formula | 6.1 | Low |

### P1 — High Impact

| # | Issue | Section | Effort |
|---|-------|---------|--------|
| 11 | Fix date format mismatch in training pipeline | 2.1 | Low |
| 12 | Add `threading.Lock()` to EDGAR throttle | 2.2 | Low |
| 13 | Consolidate ticker resolver versions | 3.1 | High |
| 14 | Fix empty-string suffix stripping | 3.3 | Low |
| 15 | Add `mc > 0` check to V4 CAGE tier | 3.2 | Trivial |
| 16 | Add cage_code to sam_poller.py output | 7.2 | Medium |
| 17 | Fix optimizer ranking metric (expectancy or Sharpe) | 6.3 | Low |
| 18 | Add real-time P&L to GUI positions table | 8.1 | High |
| 19 | Fix agency empty-string scoring bug | 4.4 | Low |
| 20 | Add process supervision for live pipeline | 7.9 | Medium |

### P2 — Important

| # | Issue | Section | Effort |
|---|-------|---------|--------|
| 21 | Add GUI order status monitoring | 8.2 | High |
| 22 | Make dashboard real-time | 8.3 | Medium |
| 23 | Add alert/notification system | 8.6 | Medium |
| 24 | Add gap risk modeling to backtest | 6.9 | Medium |
| 25 | Add liquidity checks | 6.10 | Medium |
| 26 | Add position sizing / risk management | 10.4 | High |
| 27 | Pin dependency versions | 9.7 | Low |
| 28 | Add tests for core pipeline (filter, score, trade) | 9.6 | High |
| 29 | Use Pydantic models for data validation | 9.4 | High |
| 30 | Centralize rate limiting | 9.1 | Medium |

### P3 — Nice to Have

| # | Issue | Section | Effort |
|---|-------|---------|--------|
| 31 | Fix hot sector NAICS prefix matching | 10.5 | Low |
| 32 | Add ticker format validation | 3.11 | Low |
| 33 | Add GLEIF rate limiting | 3.13 | Low |
| 34 | Improve EDGAR map TTL (7d -> 1d) | 3.10 | Low |
| 35 | Add comprehensive type hints | 9.5 | Medium |
| 36 | Normalize timezone handling | 9.8 | Medium |
| 37 | Split GUI into modules | 8.8 | High |
| 38 | Add CI/CD pipeline | 9.6 | Medium |

---

## Appendix: Issue Count by Subsystem

| Subsystem | Critical | High | Medium | Low | Total |
|-----------|----------|------|--------|-----|-------|
| Security | 2 | 0 | 0 | 0 | 2 |
| Training Pipeline | 2 | 1 | 5 | 2 | 10 |
| Ticker Resolution | 3 | 3 | 5 | 4 | 15 |
| Scoring Engine | 2 | 1 | 2 | 1 | 6 |
| Filter Engines | 1 | 1 | 3 | 0 | 5 |
| Backtest/Optimizer | 5 | 2 | 4 | 3 | 14 |
| Live Trading | 7 | 1 | 3 | 1 | 12 |
| GUI | 4 | 1 | 3 | 2 | 10 |
| Cross-Cutting | 1 | 2 | 5 | 3 | 11 |
| Conceptual | 1 | 2 | 2 | 1 | 6 |
| **Total** | **28** | **14** | **32** | **17** | **91** |
