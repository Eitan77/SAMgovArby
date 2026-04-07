# Pipeline Bugs & Issues Analysis

**Date:** 2026-04-01
**Scope:** Full dataset build and training pipeline — `build_training_set.py`, `ticker_resolver_v4.py`, `filter_engine_bt.py`, `scoring_engine.py`, `backtest.py`, `config.py`, `sam_gov_contracts.py`, `sam_gov_reader.py`

---

## Summary Table

| # | Severity | Component | Short Description |
|---|----------|-----------|-------------------|
| 1 | **CRITICAL** | `build_training_set.py` Stage 3 | Date format mismatch → all OHLC prices silently null |
| 2 | **CRITICAL** | `filter_engine_bt.py` | `"low_medium"` confidence not in enum → V4 resolutions silently rejected |
| 3 | **CRITICAL** | `ticker_resolver_v4.py` Tier 1 | GLEIF API unreachable (DNS failure) → 8.2% resolution vs. 50%+ expected |
| 4 | **HIGH** | `ticker_resolver_v4.py` Tier 1 | Possible wrong GLEIF response key — Tier 1 would still fail even with network |
| 5 | **HIGH** | `build_training_set.py` Stage 3 | `has_pr` / `first_pr_date` never populated — 15-pt scoring factor always 0 |
| 6 | **HIGH** | `backtest.py` | Imports V2 `resolve_ticker` instead of V4 — different resolution logic |
| 7 | **HIGH** | `backtest.py` | Stage 1 checkpoint key mismatch — funnel breakdown always shows zeroes |
| 8 | **HIGH** | `ticker_resolver_v4.py` Tier 1 | `ConnectionError` bails immediately — doesn't try remaining name variants |
| 9 | **HIGH** | `config.py` | SAM.gov API key hardcoded in source code |
| 10 | **MEDIUM** | `sam_gov_contracts.py` | `naics_code` always empty — hot-sector scoring always 0 for API-sourced data |
| 11 | **MEDIUM** | `build_training_set.py` Stage 2 | No-op if `records_by_key=None` — zero resolution when called standalone |
| 12 | **MEDIUM** | `build_training_set.py` Stage 3 | `shares_outstanding_approx` stores current shares, not historical |
| 13 | **MEDIUM** | `config.py` | Two SAM.gov API key variables — one unused, one hardcoded |
| 14 | **MEDIUM** | `sam_gov_contracts.py` | Duplicate field mapping for parent name |
| 15 | **LOW** | `build_training_set.py` | `_parse_bulk_row` is dead code — never called |
| 16 | **LOW** | `backtest.py` | `training_csv=None` passed to `_build_funnel_breakdown` in non-training mode |
| 17 | **LOW** | `sam_gov_reader.py` | Header detection scans for literal `"CAGE Code"` — fragile to format changes |
| 18 | **LOW** | `build_training_set.py` | Misleading Stage 3 comment contradicts actual date format in use |

---

## Issue 1 — CRITICAL: Stage 3 Date Format Mismatch — All OHLC Prices Silently Null

**File:** `build_training_set.py`, lines ~789–868
**Stage:** Stage 3 (Enrich)

### What Happens

Stage 1 outputs `posted_date` in **YYYY-MM-DD** format. This is set by `_record_to_award_dict` (line 245) from `record.posted_date`, which itself comes from `sam_gov_reader._parse_date` (line 152) — returning `raw[:10]`, e.g. `"2023-03-15"`.

Stage 3 has two separate date-parsing code blocks:

**Pre-fetch block (lines 789–795):**
```python
parts = date_str.split('/')
year = int(parts[2]) if len(parts) == 3 else int(date_str[:4])
```
This has a fallback: if the split doesn't produce 3 parts, it takes `int(date_str[:4])` — which correctly extracts `2023` from `"2023-03-15"`. The pre-fetch **works**.

**Per-award block (lines 862–868):**
```python
# Convert M/D/YYYY or MM/DD/YYYY to YYYY-MM-DD
parts = date_str.split('/')
if len(parts) == 3:
    m, d, y = parts
    normalized_date = f"{y}-{int(m):02d}-{int(d):02d}"
    year_key = (ticker, int(y))
```
There is **no fallback here**. If `date_str` is `"2023-03-15"`, `split('/')` produces one element, `len(parts) == 3` is `False`, and `normalized_date` and `year_key` are never assigned — they stay `None` from their initialization two lines earlier.

**Result:**
- `hist_df = history_cache.get(None)` → `None`
- `prices = _slice_price_window(None, None)` → `{}`
- Every `open_t0` through `return_t7` is `""`
- `t0_price = 0`, so `hist_mcap = 0`

Every award's OHLC enrichment silently produces empty data. The historical market cap is also zero, so `filter_engine_bt.py` Filter 3 ("No historical market cap data") rejects every enriched row in the backtest. The comment on line 791 even says `# date_str is in M/D/YYYY format` which is factually wrong — it's YYYY-MM-DD.

---

## Issue 2 — CRITICAL: `"low_medium"` Confidence Not in Filter Enum

**File:** `filter_engine_bt.py`, line 14
**Stage:** Backtest filtering

### What Happens

`_CONFIDENCE_LEVELS` defines the valid ordered set of confidence values:
```python
_CONFIDENCE_LEVELS = ["none", "low", "medium", "medium_high", "high"]
```

`ticker_resolver_v4.py` (line 360) emits `"low_medium"` for fuzzy matches that score ≥ 80 but have no CIK and no independent validation:
```python
return self._make_result(name, norm, ticker, "", "low_medium",
                          f"fuzzy_score_{int(score)}", None, mc)
```

When `_confidence_meets_minimum("low_medium", "medium")` is called in the backtest filter:
```python
try:
    return _CONFIDENCE_LEVELS.index(confidence) >= _CONFIDENCE_LEVELS.index(minimum)
except ValueError:
    return False  # "low_medium" not in list → raises ValueError → returns False
```

Every award resolved with `"low_medium"` confidence is silently rejected. There is no warning, no log entry — it looks identical to a legitimate confidence failure. This compounds Issue 1: even if OHLC prices were present, these records would never reach the backtest.

---

## Issue 3 — CRITICAL: Tier 1 GLEIF API Unreachable (DNS Failure)

**File:** `ticker_resolver_v4.py`, `docs/TIER1_RESOLVER_ANALYSIS.md`
**Stage:** Stage 2 (Ticker Resolution)

### What Happens

Tier 1 performs: `CAGE code → GLEIF API → LEI → OpenFIGI → Ticker`. The GLEIF endpoint (`leilookup.gleif.org`) fails DNS resolution in the current environment:

```
[Errno 11001] getaddrinfo failed
```

The code catches `ConnectionError` and returns `{}`, which causes the resolver to fall through to Tiers 2–4 (EDGAR-only). EDGAR covers approximately 8–10% of small-cap federal contractors, while Tier 1 would add an estimated 35–50% coverage. The current resolution rate is **8.2%**, versus an expected **45–55%** with Tier 1 working.

The failure is a network/environment constraint — the code itself handles the error correctly. However, the entire pipeline is built around expected coverage that's currently unachievable.

---

## Issue 4 — HIGH: GLEIF Response Key Likely Wrong — Tier 1 Would Fail Even With Network

**File:** `ticker_resolver_v4.py`, line 261

### What Happens

In `_resolve_via_cage`, after a successful HTTP 200 from the GLEIF API:
```python
data = resp.json()
records = data.get("lei_records", [])
```

The GLEIF API v3 (`/api/v3/lei-records`) wraps its results in a `"data"` key, not `"lei_records"`. The correct extraction should be `data.get("data", [])`. Using the wrong key means `records` is always `[]`, so the inner loop over LEI records never executes, and Tier 1 never resolves any ticker regardless of network availability.

This means even if the DNS issue (Issue 3) were fixed, Tier 1 would still produce zero resolutions.

---

## Issue 5 — HIGH: `has_pr` / `first_pr_date` Never Populated — 15-pt Factor Always 0

**File:** `build_training_set.py`, Stage 3, lines 923–925

### What Happens

In the Stage 3 enrichment loop:
```python
"first_pr_date":  "",
"has_pr":         "unknown",
```
This is hardcoded. The comment reads: `# PR (not yet implemented — mark as unknown so scoring doesn't give free points)`.

In `scoring_engine.py` (lines 72–78):
```python
if has_press_release is True:
    pts = 0
elif has_press_release is False:
    pts = w["no_pr"]   # 15 pts
else:
    pts = 0            # None/unknown → conservative: 0 pts
```

And in `filter_engine_bt.py` (lines 106–116), `has_pr = "unknown"` always sets `extra["has_press_release"] = None`.

**Result:** The "no press release" factor — worth 15 out of 100 points — is **permanently zero** for every row in the training set. This systematically deflates all scores and changes which contracts pass the 40-point threshold. The scoring model is operating with a structural hole that makes backtest results incomparable to any future live run where PR data is present.

---

## Issue 6 — HIGH: `backtest.py` Imports V2 Resolver Instead of V4

**File:** `backtest.py`, line 26

### What Happens

```python
from ticker_resolver import resolve_ticker   # V2
```

The main training pipeline (`build_training_set.py`) uses `TickerResolverV4` with CAGE/UEI/LEI resolution. `backtest.py` in non-training mode calls `resolve_ticker` from the original V2 (`ticker_resolver.py`), which:

- Has no CAGE code support
- Uses an older fuzzy matching threshold (80/85 vs. 70/75 in V4)
- Has no multi-name fallback (legal, dba, parent)
- Does not emit `"low_medium"` confidence (different confidence vocabulary)

Any backtest run that doesn't use the `training_csv` path uses a fundamentally different resolution engine than the one that built the training data. The two systems are not equivalent, so backtest results from non-training mode are not comparable to training mode results.

---

## Issue 7 — HIGH: Stage 1 Checkpoint Keys Mismatched With Backtest Funnel

**File:** `backtest.py`, lines 88–92 vs. `build_training_set.py`, lines 316–320

### What Happens

`build_training_set.py` saves Stage 1 checkpoint with these keys:
```python
{
    "total_rows_read": total_rows,
    "after_load": after_load,
    "final_count": len(awards),
}
```

`backtest.py` reads this checkpoint expecting **different keys**:
```python
breakdown["after_dedup_amount"] = cp1.get("unique_after_dedup_and_amount_filter", 0)
breakdown["stage1_top20"]       = cp1.get("dropped_top20", 0)
breakdown["stage1_idiq"]        = cp1.get("dropped_idiq", 0)
breakdown["stage1_total"]       = cp1.get("final_count", 0)  # ← only this one matches
```

Only `"final_count"` exists in the saved checkpoint. The keys `unique_after_dedup_and_amount_filter`, `dropped_top20`, and `dropped_idiq` don't exist in the actual checkpoint (and `dropped_top20`/`dropped_idiq` would require Stage 1 to track those separately — it never did). The GUI funnel breakdown always shows `0` for those three fields.

Additionally, there is no top-20 company removal logic in `stage1_load_and_filter` at all, despite the CLAUDE.md documentation and backtest.py expecting a `dropped_top20` value.

---

## Issue 8 — HIGH: Tier 1 `ConnectionError` Bails Immediately — Doesn't Try Other Names

**File:** `ticker_resolver_v4.py`, lines 278–281

### What Happens

The Tier 1 loop iterates over multiple name variants to try against GLEIF. When a `ConnectionError` is caught:
```python
except requests.exceptions.ConnectionError as e:
    log.debug(f"Tier 1 GLEIF unreachable (network): {type(e).__name__}")
    return {}   # ← exits the entire _resolve_via_cage method
```

This is inside the outer `for name in names_to_try:` loop. On the first `ConnectionError` (which is the first name tried, since the network is unreachable), the method returns immediately instead of continuing the loop. The intent was probably to avoid retrying a permanently unavailable network, but:

1. `Timeout` exceptions (line 282–284) correctly `continue` to the next name — the network *policy* is inconsistent: timeouts retry, connection errors don't.
2. Because the `requests.get` call is inside both the `for name` loop and the inner `for search_name in [name, name.split()[:2]]` loop, a connection error on the first variation of the first name aborts all remaining names and variations.

This means if the GLEIF API were intermittently reachable, a transient failure on the first name variant would cause the entire Tier 1 attempt to be abandoned, even though other name variations might succeed.

---

## Issue 9 — HIGH: SAM.gov API Key Hardcoded in Source Code

**File:** `config.py`, line 36

### What Happens

```python
SAM_GOV_API_KEY = "SAM-178836eb-f9ad-4c50-9872-dc258dba2521"  # WARN: Do not commit this to git
```

The API key is committed directly into the Python source file. The comment warns against this, yet it is done. If this repository is pushed to a remote or shared, the key is exposed. The key should be in a `.env` file and loaded via `os.getenv("SAM_GOV_API_KEY")`.

`sam_gov_contracts.py` reads this directly:
```python
from config import SAM_GOV_API_KEY as DEFAULT_API_KEY
```
It does check `os.getenv("SAM_GOV_API_KEY")` first, but falls back to the hardcoded value.

---

## Issue 10 — MEDIUM: `sam_gov_contracts.py` Always Sets `naics_code` Empty

**File:** `sam_gov_contracts.py`, line 173

### What Happens

The `_normalize_record` method maps SAM.gov API response fields to a USASpending-compatible schema. The NAICS code field is hardcoded:
```python
"naics_code": "",  # SAM.gov may not include NAICS in same way
```

Any contracts sourced through `SamGovClient` (the live/API path) will always have an empty NAICS code. The scoring engine's hot-sector check (`scoring_engine.py` lines 62–68):
```python
naics = contract.get("naics", "")
if naics in HOT_SECTOR_NAICS:
    pts = w["hot_sector"]          # 15 pts
elif naics.startswith(GENERAL_DEFENSE_NAICS_PREFIX):
    pts = int(w["hot_sector"] * 0.53)  # 8 pts
else:
    pts = 0
```
...will always produce 0 for API-sourced contracts. The sole-source, 8-K, and set-aside fields are also empty in `_normalize_record`, making the API client largely unusable for accurate scoring.

---

## Issue 11 — MEDIUM: Stage 2 Is a No-Op When Called Without `records_by_key`

**File:** `build_training_set.py`, `stage2_resolve_tickers`, lines 449–455

### What Happens

```python
ek_to_record: dict[str, ContractRecord] = {}
if records_by_key:
    for ek, award_keys in entity_key_to_award_keys.items():
        for ak in award_keys:
            if ak in records_by_key:
                ek_to_record[ek] = records_by_key[ak]
                break
```

`TickerResolverV4.resolve()` requires a `ContractRecord` object (not a plain dict), because it accesses `record.cage_code`, `record.uei`, `record.legal_business_name`, etc. If `records_by_key` is `None` or empty — which happens if Stage 2 is called independently without running Stage 1 first — `ek_to_record` is empty.

Then in the resolution loop (line 464–466):
```python
record = ek_to_record.get(ek)
if record is None:
    unresolved_count += 1
    entry = {"ticker": "", "cik": "", "ticker_confidence": "none"}
```

Every entity gets an empty entry. Zero resolution happens. No error is raised. The output CSV looks structurally valid but contains no tickers. There is no guard or warning for this scenario.

---

## Issue 12 — MEDIUM: `shares_outstanding_approx` Stores Current Shares, Not Historical

**File:** `build_training_set.py`, Stage 3, line 915

### What Happens

The enrichment data includes two shares fields:
```python
"shares_outstanding_approx":    shares_cache.get(ticker, 0),    # CURRENT shares
"shares_outstanding_historical": hist_shares,                    # historical (correct)
```

`shares_cache` is populated from `_get_shares(ticker)` which calls `yf.Ticker(ticker).info["sharesOutstanding"]` — the **current** share count, not the historical one. For awards from 2023, the current share count could differ significantly from the 2023 count due to dilutive offerings, buybacks, or splits that occurred between 2023 and today. The field name `shares_outstanding_approx` implies historical approximation, but it's actually current.

The historical market cap calculation (line 887) correctly uses `hist_shares` (not the approx field), so the computed `historical_market_cap_approx` is valid. But the `shares_outstanding_approx` column itself is misleading and any downstream user of that column gets current data labeled as historical.

---

## Issue 13 — MEDIUM: Two SAM.gov API Key Variables, One Unused

**File:** `config.py`, lines 7 and 36

### What Happens

```python
SAM_API_KEY = os.getenv("SAM_API_KEY")          # line 7 — reads from env, never used
...
SAM_GOV_API_KEY = "SAM-178836eb-..."             # line 36 — hardcoded, actually used
```

`SAM_API_KEY` (line 7) is defined as an env var read, but nothing in the codebase imports or uses this variable. The actually-used key is `SAM_GOV_API_KEY` (line 36). The intended design was probably to have `SAM_GOV_API_KEY = os.getenv("SAM_GOV_API_KEY")`, matching the env var checked in `sam_gov_contracts.py`. Instead there are two separate variables with different names, one of which is a dead assignment.

---

## Issue 14 — MEDIUM: Duplicate Parent Name Field Mapping in `sam_gov_contracts.py`

**File:** `sam_gov_contracts.py`, lines 182–183

### What Happens

```python
"recipient_parent_name": awardee_uei.get("awardeeUltimateParentName", "").strip(),
"parent_recipient_name": awardee_uei.get("awardeeUltimateParentName", "").strip(),
```

Both `recipient_parent_name` and `parent_recipient_name` are set to the exact same source field. These appear to be two different naming conventions used in different parts of the codebase (USASpending CSVs use `recipient_parent_name`; internal code sometimes uses `parent_recipient_name`). Rather than picking one canonical name, both are emitted with identical values, creating silent duplication. Any code that joins on one but not the other will silently get the right answer, masking the schema inconsistency.

---

## Issue 15 — LOW: `_parse_bulk_row` Is Dead Code

**File:** `build_training_set.py`, lines 159–233

### What Happens

`_parse_bulk_row` parses rows from the **old USASpending bulk CSV format** (with columns like `award_or_idv_flag`, `current_total_value_of_award`, `recipient_name`, etc.). The current pipeline migrated to SAM.gov bulk CSV format parsed by `sam_gov_reader.py` and converted by `_record_to_award_dict`.

`stage1_load_and_filter` only calls `read_sam_gov_csv` + `_record_to_award_dict`. `_parse_bulk_row` is never called anywhere in the codebase. It's dead code occupying 75 lines and referencing column names that don't exist in the current data source. If someone mistakenly calls it on the current SAM.gov CSV, it will silently discard all rows because:
- `row.get("award_or_idv_flag")` will be empty
- `row.get("recipient_name")` won't exist (SAM.gov uses `"Contractor Name"`)
- `row.get("contract_award_unique_key")` won't exist (SAM.gov uses `"PIID"`)

---

## Issue 16 — LOW: `training_csv=None` Passed to `_build_funnel_breakdown` in Non-Training Mode

**File:** `backtest.py`, line 263

### What Happens

At the end of the non-training backtest path:
```python
breakdown = _build_funnel_breakdown(all_results, training_csv=None)
```

`_build_funnel_breakdown` has a branch for Stage 3 (line 112):
```python
if training_csv and os.path.exists(training_csv):
    training_rows = ...
    breakdown["stage3_after_enrich"] = training_rows
```

With `training_csv=None`, this branch is skipped, so `stage3_after_enrich` is always 0 in the GUI funnel when running the standard (non-training) backtest. The GUI will always display an incorrect 0 for Stage 3 row count in non-training mode, even when the training CSV exists on disk.

---

## Issue 17 — LOW: CSV Header Detection Relies on Literal String Match

**File:** `sam_gov_reader.py`, lines 86–92

### What Happens

The SAM.gov CSV has a multi-line preamble (report title, blank lines, filter metadata) before the actual headers. The reader skips the preamble by scanning for the header row:
```python
for raw_line in f:
    if "CAGE Code" in raw_line:
        header_line = raw_line
        break
```

If SAM.gov ever renames the column (e.g., to `"Cage Code"`, `"CAGE code"`, or the column is absent in a future export format), the header detection silently fails — `header_line` stays `None`, the function returns with no data, and Stage 1 reports 0 rows read without raising an error. There is no warning emitted when `header_line is None`. The pipeline continues with an empty awards list, writes an empty CSV, and checkpoints `final_count: 0`.

---

## Issue 18 — LOW: Misleading Comment Contradicts Actual Date Format

**File:** `build_training_set.py`, line 791

### What Happens

```python
# date_str is in M/D/YYYY format (e.g., "9/24/2021")
parts = date_str.split('/')
year = int(parts[2]) if len(parts) == 3 else int(date_str[:4])
```

The comment is wrong — `date_str` is YYYY-MM-DD as set by Stage 1. This wrong comment is what likely caused Issue 1 (the per-award block further down assumes M/D/YYYY because the developer believed this comment). The fallback `int(date_str[:4])` in the pre-fetch block handles the real format correctly by accident, making the pre-fetch work while the per-award normalization block does not.

---

## Cross-Cutting Issue: Confidence Level Vocabulary Is Fragmented

Three different confidence vocabularies exist across the codebase with no shared enum or validation:

| Location | Confidence Values Used |
|----------|------------------------|
| `ticker_resolver_v4.py` | `"none"`, `"low"`, `"medium"`, `"medium_high"`, `"high"`, `"low_medium"` |
| `filter_engine_bt.py` `_CONFIDENCE_LEVELS` | `"none"`, `"low"`, `"medium"`, `"medium_high"`, `"high"` |
| `ticker_resolver.py` (V2) | `"high"`, `"medium"`, `"low"` (no "none", no "medium_high") |

There is no shared constant or enum defining valid confidence levels. V4 emits a value (`"low_medium"`) that the filter doesn't know about (Issue 2). V2 used in `backtest.py` (Issue 6) has yet another vocabulary. Any future change to resolver confidence strings will silently break filtering without any error.

---

## Compounding Effect: What Actually Reaches the Backtest

Combining the above issues, a record from the training CSV must survive all of the following to contribute any backtest signal:

1. **Stage 2** resolves the ticker → 8.2% of records get a ticker (Issues 3 & 4 block Tier 1)
2. **Confidence filter** passes → any V4 `"low_medium"` result is silently rejected (Issue 2)
3. **Historical market cap > 0** → requires `hist_mcap` from OHLC t0 price × historical shares, but t0 price is always empty (Issue 1) → `hist_mcap = 0` → **Filter 3 rejects every enriched row**
4. **Score ≥ 40** → `no_pr` factor is permanently 0 (Issue 5), so max score without PR data is 85/100, but many contracts that would score 40+ with PR data will score below threshold

In practice, the current pipeline produces a training CSV where enriched price columns are all empty, historical market cap is always 0, and the backtest filter rejects every ticker-resolved row at Filter 3 ("No historical market cap data"). The backtest produces 0 trades from the training CSV path.
