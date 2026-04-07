# SAMgovArby Implementation Plan

**Date:** 2026-04-05
**Purpose:** Fix all verified issues from PROJECT_AUDIT. Hand off to Claude Code for implementation.

**User Constraints:**
- API key STAYS hardcoded (skip 1.1)
- PR always false for backtesting is DESIRED behavior (skip 4.2 no_pr adjustments)
- REMOVE all Sharpe ratio code entirely
- REMOVE all legacy/dead code

---

## Phase 1: Dead Code & Legacy Removal

### 1A. Delete Dead Files
**Files to delete:**
- `sam_gov_contracts.py` — dead code, never imported anywhere. `sam_poller.py` is the active module.
- `ticker_resolver.py` — V1/V2 resolver, superseded by V4. Only V4 is used in production.
- `ticker_resolver_v3.py` — V3 resolver, superseded by V4. Only V4 is used in production.
- `.ticker_cache_v2.json` — V2 cache file, orphaned after V2 removal.

**Verification before deleting:** Grep entire codebase for imports of each file. Confirm zero active imports.

### 1B. Delete Dead Function: `apply_filters_bt()`
**File:** `filter_engine_bt.py:140-193`
**What:** The generic `apply_filters_bt()` function is never called. Only `apply_filters_bt_from_training()` is used (by `backtest.py` and `optimizer.py`).
**Action:** Delete the entire `apply_filters_bt()` function.

### 1C. Remove All Sharpe Ratio Code
**File:** `backtest.py:375-382` and anywhere Sharpe is computed or displayed.
- Delete the Sharpe ratio calculation in `backtest.py` (`sharpe = (mean / std * math.sqrt(...)`)
- Remove Sharpe from the results dict returned by backtest
- Remove Sharpe from any summary/display output in `backtest.py`
- Remove Sharpe from `optimizer.py` if it references it in ranking or output
- Remove Sharpe from `gui.py` if displayed in any panel or table
- Grep entire project for `sharpe` (case-insensitive) and remove all references

### 1D. Remove Unused Config Variable
**File:** `config.py:7`
- **DO NOT DELETE** `SAM_API_KEY = os.getenv("SAM_API_KEY")` — audit was wrong, it IS imported by `sam_poller.py` and `historical_poller.py`.
- **DO DELETE** the dead config variable `SAM_API_KEY` only if confirmed unused after re-check. (Audit finding 1.2 was INCORRECT — this variable is used.)

### 1E. Remove Duplicated EDGAR URL
**Files:** `edgar_client.py:13`, `ticker_resolver_v3.py:73` (but V3 is being deleted in 1A)
- After deleting `ticker_resolver_v3.py`, the duplication is resolved automatically.
- Keep `SUBMISSIONS_URL` in `edgar_client.py` as the single source of truth.

---

## Phase 2: Critical Bug Fixes

### 2A. Fix Date Format Mismatch in Training Pipeline
**File:** `build_training_set.py:791-793` and `build_training_set.py:862-866`
**Problem:** `sam_gov_reader.py` returns dates in `YYYY-MM-DD` format, but `build_training_set.py` Stage 3 assumes `M/D/YYYY` and splits on `/`.
**Fix:** Update the date normalization code in `build_training_set.py` to handle BOTH formats:
```python
def _normalize_date(date_str):
    """Normalize date string to YYYY-MM-DD format.
    Accepts: YYYY-MM-DD, M/D/YYYY, MM/DD/YYYY"""
    if not date_str:
        return None
    date_str = date_str.strip()
    if '-' in date_str and len(date_str) >= 8:
        # Already YYYY-MM-DD
        parts = date_str.split('-')
        if len(parts) == 3 and len(parts[0]) == 4:
            return date_str[:10]
    if '/' in date_str:
        parts = date_str.split('/')
        if len(parts) == 3:
            m, d, y = parts
            return f"{y}-{int(m):02d}-{int(d):02d}"
    return None
```
Apply this function everywhere dates are parsed in Stage 3 (lines ~791-793 and ~862-866). Replace the inline split logic with a call to `_normalize_date()`.

### 2B. Fix EDGAR Throttle Thread Safety
**File:** `build_training_set.py:94-102`
**Problem:** `_edgar_last` global is read/written by multiple threads without synchronization.
**Fix:** Add a `threading.Lock()`:
```python
import threading

_edgar_lock = threading.Lock()
_edgar_last = 0.0

def _edgar_throttle():
    global _edgar_last
    with _edgar_lock:
        elapsed = time.time() - _edgar_last
        if elapsed < EDGAR_RATE_LIMIT_SEC:
            time.sleep(EDGAR_RATE_LIMIT_SEC - elapsed)
        _edgar_last = time.time()
```

### 2C. Don't Cache Failed EDGAR Responses
**File:** `build_training_set.py:894`
**Problem:** Failed EDGAR fetches return `{}` and get cached permanently in `submissions_cache`. Same CIK never retried.
**Fix:** Only cache non-empty results:
```python
result = _fetch_edgar_submissions(cik)
if result:  # Only cache successful fetches
    submissions_cache[cik] = result
subs = submissions_cache.get(cik, {})
```

### 2D. Fix Sole-Source Type Validation in Scoring
**File:** `scoring_engine.py:45`
**Problem:** `if contract.get("sole_source")` treats string `"False"` as truthy, awarding 25 pts incorrectly.
**Fix:**
```python
sole_source_val = contract.get("sole_source")
# Handle bool, string "True"/"False", and truthy/falsy values
if isinstance(sole_source_val, str):
    sole_source = sole_source_val.lower() in ("true", "1", "yes")
else:
    sole_source = bool(sole_source_val)
pts = w["sole_source"] if sole_source else 0
```

### 2E. Fix Agency Empty String Scoring Bug
**File:** `scoring_engine.py:52-56`
**Problem:** Empty string `""` agency is never in `agency_history`, so missing data awards full 15 pts.
**Fix:**
```python
agency = contract.get("agency", "")
if not agency or agency_history is None:
    pts = 0  # No data = no points
else:
    pts = w["first_agency"] if agency not in agency_history else 0
```

### 2F. Fix Hot Sector Rounding
**File:** `scoring_engine.py:66`
**Problem:** `int(15 * 0.53) = 7` instead of intended 8.
**Fix:** Change `int(w["hot_sector"] * 0.53)` to `round(w["hot_sector"] * 0.53)`.

### 2G. Fix `time_in_force` for Bracket Orders
**File:** `trade_executor.py:95`
**Problem:** `time_in_force="day"` cancels bracket orders at market close, contradicting multi-day hold strategy (`MAX_HOLD_DAYS=4`).
**Fix:** Change `time_in_force="day"` to `time_in_force="gtc"`.

### 2H. Fix CAGE Tier Missing Market Cap Validation (V4)
**File:** `ticker_resolver_v4.py:268-275`
**Problem:** CAGE resolution path returns ticker without checking `mc > 0`. All other tiers check this.
**Fix:** Add `if mc > 0` check before returning, matching other tiers:
```python
mc = self._get_market_cap(ticker)
if mc > 0:
    return TickerResult(ticker=ticker, confidence=confidence, source=source, market_cap=mc)
# Fall through to next resolution tier if mc <= 0
```

### 2I. Fix Empty String After Suffix Stripping (V3 is being deleted, but check V4)
**File:** `ticker_resolver_v4.py` — check if V4 has same `_strip_suffixes()` bug.
**Action:** Search V4 for suffix stripping. If present, add guard:
```python
def _strip_suffixes(name: str) -> str:
    words = name.split()
    while words and words[-1] in _SUFFIX_WORDS:
        words.pop()
    result = " ".join(words)
    return result if result else name  # Return original if all words were suffixes
```

### 2J. Fix LEI Resolver Confidence Boosting Bug
**File:** `lei_resolver.py:69-74`
**Problem:** Confidence boosted by +0.1 when OpenFIGI succeeds, BEFORE GLEIF validation. Can reach 1.0 with failed GLEIF.
**Fix:** Move confidence boost AFTER GLEIF validation succeeds:
```python
if result["ticker"]:
    gleif_info = self._get_gleif_info(lei_upper)
    if gleif_info:
        # Only boost confidence after GLEIF validates the entity
        result["confidence"] = min(1.0, result["confidence"] + 0.1)
```
Actually — re-read the code carefully. If the boost is on line 74 inside `if gleif_info:`, it IS after GLEIF. The audit says it's "before GLEIF validation" but the code nesting may say otherwise. **Verify the actual nesting before changing.** If the boost is correctly nested inside `if gleif_info:`, this is a false positive and skip.

---

## Phase 3: Live Trading Hardening

### 3A. Add Ticker Confidence Filtering in Live Mode
**File:** `main.py:127-142`
**Problem:** Live pipeline accepts any confidence level. Backtest filters at `MIN_TICKER_CONFIDENCE = "medium"`.
**Fix:** After ticker resolution, add confidence check:
```python
ticker, confidence = resolve_ticker(...)
if not ticker:
    continue

# Apply same confidence filter as backtest
confidence_levels = ["low", "low_medium", "medium", "medium_high", "high"]
min_idx = confidence_levels.index(MIN_TICKER_CONFIDENCE)
if confidence in confidence_levels and confidence_levels.index(confidence) < min_idx:
    logger.info(f"Skipping {ticker}: confidence {confidence} below {MIN_TICKER_CONFIDENCE}")
    continue
```

### 3B. Validate Alpaca Order Response
**File:** `trade_executor.py:90-99`
**Problem:** After `api.submit_order()`, no check that order was accepted.
**Fix:**
```python
order = api.submit_order(...)
if not order or not getattr(order, 'id', None):
    logger.error(f"Order submission failed for {ticker}")
    return None
if getattr(order, 'status', '') == 'rejected':
    logger.error(f"Order rejected for {ticker}: {getattr(order, 'reject_reason', 'unknown')}")
    return None
```

### 3C. Add `cage_code` to SAM Poller Output
**File:** `sam_poller.py`
**Problem:** CAGE code not included in contract dict. V4 resolver's highest-confidence CAGE tier is skipped in live mode.
**Fix:** Find the CAGE code field in the SAM.gov API response and include it in the returned contract dict. The SAM Opportunities API v2 field is likely `awardee.cageCode` or similar. Check the API response structure and add:
```python
"cage_code": opp.get("awardee", {}).get("cageCode", ""),
```

### 3D. Fix Processed Awards Dedup Key
**File:** `main.py:171-182`
**Problem:** Dedup key uses `(awardee_name, award_amount, posted_date)` — collides for same-day same-amount contracts from different agencies.
**Fix:** Add agency to the dedup key to reduce collision risk:
```python
def _award_key(contract):
    return _json.dumps([
        contract["awardee_name"],
        str(contract["award_amount"]),
        contract.get("posted_date", "")[:10],
        contract.get("agency", ""),
        contract.get("solicitation_number", ""),
    ], separators=(",", ":"))
```

### 3E. Lazy Alpaca Client Validation
**File:** `trade_executor.py:30`
**Problem:** Missing credentials not caught until first trade attempt.
**Fix:** Add credential validation at module load:
```python
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    import warnings
    warnings.warn("Alpaca credentials not set. Live trading will fail.")
```
Keep `get_api()` lazy, but warn at import time.

---

## Phase 4: Backtest & Optimizer Improvements

### 4A. Add Slippage and Commission to Backtest
**File:** `price_sim.py`
**Problem:** Entry/exit prices are exact OHLC with no trading costs.
**Fix:** Add configurable slippage and commission parameters. Apply slippage to entry price (add for buys) and exit price (subtract for sells):
```python
# In config.py, add:
SLIPPAGE_PCT = 0.005     # 0.5% slippage per trade
COMMISSION_PCT = 0.001   # 0.1% commission per trade (round-trip = 0.2%)

# In price_sim.py, after determining entry_price:
entry_price = open_t0 * (1 + SLIPPAGE_PCT)  # Worse fill for buy

# For exit (TP/SL/hold expiry), apply slippage in the negative direction:
exit_price = actual_exit_price * (1 - SLIPPAGE_PCT)

# After computing raw P&L:
commission = (entry_price + exit_price) * COMMISSION_PCT
pnl = (exit_price - entry_price) - commission
pnl_pct = pnl / entry_price
```

### 4B. Fix Optimizer Ranking Metric
**File:** `optimizer.py:259-268`
**Problem:** `_rank_score()` returns total P&L percentage, rewarding more trades not better trades.
**Fix:** Replace with expectancy-based ranking:
```python
def _rank_score(stats: dict) -> float:
    """Rank by expectancy (avg pnl per trade) * sqrt(num_trades)."""
    n = stats.get("total_trades", 0)
    if n == 0:
        return -999
    avg_pnl = stats.get("total_pnl_pct", 0) / n
    # Reward consistent edge: expectancy * sqrt(n) balances quality and quantity
    return avg_pnl * math.sqrt(n)
```

### 4C. Fix `optimize_from_cache()` Missing Market Cap Loop
**File:** `optimizer.py:70-75`
**Problem:** Cache-mode optimizer doesn't iterate over `max_market_cap` values unlike the training CSV mode.
**Fix:** Add `PARAM_GRID["max_market_cap"]` to the `itertools.product()` call:
```python
combos = list(itertools.product(
    PARAM_GRID["score_threshold"],
    PARAM_GRID["take_profit_pct"],
    PARAM_GRID["stop_loss_pct"],
    PARAM_GRID["max_hold_days"],
    PARAM_GRID["max_market_cap"],   # ADD THIS
))
```
Then update the loop body to unpack and apply `max_market_cap`.

### 4D. Fix Global Config Mutation in Backtest
**File:** `backtest.py:159-175`
**Problem:** Mutates `config_module.MAX_MARKET_CAP` at runtime. Thread-unsafe.
**Fix:** Pass `max_market_cap` as a parameter through the call chain instead of mutating global state:
```python
# In run_backtest(), instead of:
#   config_module.MAX_MARKET_CAP = max_market_cap
# Pass it as parameter:
results = _process_awards(awards, max_market_cap=max_market_cap, ...)

# In filter calls, pass explicitly:
passed, reason, extra = apply_filters_bt_from_training(row, max_market_cap=max_market_cap)
```
This requires updating `apply_filters_bt_from_training()` in `filter_engine_bt.py` to accept an optional `max_market_cap` parameter, defaulting to `config.MAX_MARKET_CAP` if not provided.

---

## Phase 5: Sole-Source Consolidation

### 5A. Create Single Source of Truth for Sole-Source Detection
**Problem:** Sole-source logic is duplicated in 3 places with different logic.
**Fix:** Create a single function (in an appropriate existing module, e.g., `scoring_engine.py` or `config.py`):
```python
# In config.py or a shared utils module:
SOLE_SOURCE_CODES = {"B", "C", "G", "CDO", "URG", "SP2"}
SOLE_SOURCE_INDICATORS = {"sole source", "only one source", "one responsible source", "unique source"}

def is_sole_source(extent_competed_code: str = "", description: str = "",
                   num_offers: str = "", other_than_full_open: str = "") -> bool:
    """Single source of truth for sole-source determination."""
    if extent_competed_code.upper() in SOLE_SOURCE_CODES:
        return True
    if num_offers == "1":
        return True
    desc_lower = description.lower()
    if any(ind in desc_lower for ind in SOLE_SOURCE_INDICATORS):
        return True
    otfo = (other_than_full_open or "").strip().upper()
    if otfo and otfo not in ("", "NO", "N"):
        return True
    return False
```
Then replace all 3 duplicate implementations with calls to this function.

---

## Phase 6: Thread Safety & Rate Limiting

### 6A. Centralize Rate Limiting
**Problem:** 3 identical rate-limit implementations in `edgar_client.py`, `news_checker.py`, `ticker_resolver_v3.py` (V3 deleted in Phase 1, but `build_training_set.py` has its own too).
**Fix:** Create `rate_limiter.py`:
```python
import threading
import time

class RateLimiter:
    def __init__(self, min_interval: float):
        self._min_interval = min_interval
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last = time.time()
```
Replace all 3 (remaining) rate-limit implementations with instances of `RateLimiter`:
- `edgar_client.py`: `edgar_limiter = RateLimiter(0.12)`
- `news_checker.py`: `news_limiter = RateLimiter(1.0)`
- `build_training_set.py`: `edgar_limiter = RateLimiter(EDGAR_RATE_LIMIT_SEC)`

### 6B. Thread-Safe Market Cap Cache
**File:** `filter_engine.py:13`
**Problem:** `_mcap_cache` dict accessed without locking.
**Fix:** Since Python's GIL makes dict reads/writes atomic for simple operations, this is low risk. But for safety, wrap in a lock:
```python
_mcap_lock = threading.Lock()
_mcap_cache: dict = {}

def _get_cached_mcap(ticker):
    with _mcap_lock:
        return _mcap_cache.get(ticker)

def _set_cached_mcap(ticker, value):
    with _mcap_lock:
        _mcap_cache[ticker] = value
```

---

## Phase 7: GUI Fixes

### 7A. Check Process Exit Codes
**File:** `gui.py:872`
**Fix:**
```python
def _on_pipeline_finished(self, code: int):
    self._set_running(False)
    if code != 0:
        logger.error(f"Pipeline process exited with code {code}")
        # Show error indicator in the GUI
```

### 7B. Replace Silent Exception Handlers
**File:** `gui.py` — lines 304, 429, 506, and others
**Fix:** Replace `except Exception: pass` with logging:
```python
except Exception as e:
    logger.warning(f"Non-critical error in <context>: {e}")
```
Do this for all bare `except Exception: pass` blocks in gui.py.

### 7C. Add Missing Dependencies to requirements.txt
**File:** `requirements.txt`
**Fix:** Add:
```
PyQt6>=6.5
matplotlib>=3.7
pandas_market_calendars>=4.3
```

---

## Phase 8: Minor Fixes

### 8A. Stage 2 Resume: Don't Skip New Awards for Known Entities
**File:** `build_training_set.py:438-457`
**Fix:** When resuming, still process new award keys even if the entity was previously resolved. The checkpoint should track individual award keys, not entity keys:
```python
for award in awards:
    award_key = award["award_key"]
    if award_key in cp:
        skipped_count += 1
        continue
    # Even if entity was previously resolved, this specific award needs processing
    ek = _v4_key(award)
    entity_key_to_award_keys.setdefault(ek, []).append(award_key)
```

### 8B. Historical Shares Fallback: Handle Empty Ticker
**File:** `build_training_set.py:877-885`
**Fix:** When ticker is empty, set shares to 0 with source `"missing_ticker"`:
```python
if ticker and date_str:
    hist_shares, shares_source = _get_historical_shares(ticker, date_str, shares_cache)
elif ticker:
    hist_shares, shares_source = shares_cache.get(ticker, 0), "current_only"
else:
    hist_shares, shares_source = 0, "missing_ticker"
```

### 8C. 8-K Hours Column Rename
**File:** `build_training_set.py:714`
**Fix:** Rename column from `hours_to_8k` to `days_to_8k` and store as days (not `days * 24`):
```python
# Change: hours_to_8k = delta.days * 24
# To:
days_to_8k = delta.days
```
Update all references to `hours_to_8k` throughout the codebase to `days_to_8k`.

---

## Explicitly NOT Fixing (Per User Instructions)

| Audit # | Issue | Reason Skipped |
|---------|-------|----------------|
| 1.1 | Hardcoded API key | User wants it hardcoded |
| 1.2 | Dead SAM_API_KEY | Audit was WRONG — variable IS used |
| 4.2 | Live scoring ceiling ~70 | PR=false for backtesting is desired behavior |
| 9.10 | MAX_MARKET_CAP unused | Audit was WRONG — IS used in filters |
| 6.1 | Invalid Sharpe ratio | Removing ALL Sharpe code instead of fixing |
| 10.x | Conceptual/strategic | Out of scope for code fixes |
| 8.1-8.4 | GUI real-time features | Major new feature work, not bug fixes |
| 8.6 | Alert/notification system | Major new feature work |

---

## Execution Order

Execute phases in order. Within each phase, items can be done in parallel.

1. **Phase 1** first — removes dead code, reduces codebase surface
2. **Phase 2** — critical bugs that affect correctness
3. **Phase 3** — live trading safety
4. **Phase 4** — backtest accuracy
5. **Phase 5-6** — code quality (consolidation, thread safety)
6. **Phase 7-8** — GUI and minor fixes

**After each phase:** Run `rtk python -m pytest tests/` to verify no regressions. Grep for any broken imports from deleted files.

---

## Verification Checklist

After all phases complete:
- [ ] `rtk python -m pytest tests/` passes
- [ ] `rtk python -c "from build_training_set import *"` imports cleanly
- [ ] `rtk python -c "from backtest import *"` imports cleanly
- [ ] `rtk python -c "from main import *"` imports cleanly
- [ ] `rtk python -c "from gui import *"` imports cleanly (requires PyQt6)
- [ ] Grep for `sharpe` (case-insensitive) returns zero hits
- [ ] Grep for `ticker_resolver_v3` returns zero hits
- [ ] Grep for `sam_gov_contracts` returns zero hits
- [ ] No `except Exception: pass` remains in gui.py
