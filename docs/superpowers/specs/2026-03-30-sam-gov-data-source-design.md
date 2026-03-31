# SAM.gov Data Source Migration Design

**Date:** 2026-03-30
**Status:** Design Approved
**Objective:** Replace USASpending bulk CSV data source with sam.gov API to gain access to CAGE codes for improved ticker resolution via TickerResolverV3.

---

## Problem Statement

Currently, `build_training_set.py` loads contract data from USASpending bulk CSV files. USASpending data lacks CAGE (Contractor And Government Entity) codes, which are critical for the CAGE → LEI → Ticker resolution pipeline in TickerResolverV3.

**Gap:** TickerResolverV3 supports CAGE-based ticker resolution but training data has no CAGE codes to pass to it.

**Solution:** Fetch contracts directly from sam.gov API (which includes CAGE codes), normalize to existing schema, and integrate into the pipeline with minimal changes.

---

## Architecture

### Data Flow

```
sam.gov REST API (authenticated with SAM_GOV_API_KEY)
        ↓
sam_gov_contracts.py (NEW MODULE)
├─ Handle pagination & rate limiting
├─ Normalize sam.gov schema → USASpending schema + CAGE code
├─ Checkpoint progress (offset/pagination state)
└─ Yield records one-by-one (memory efficient)
        ↓
build_training_set.py (UNCHANGED core logic)
├─ Stage 1: Load & Filter ($1M–$10B, remove IDIQ, top-20 companies)
├─ Stage 2: Ticker Resolve (now receives CAGE codes)
└─ Stage 3: Enrich (prices, shares, market cap, filings)
        ↓
Training Dataset (with CAGE codes available)
├─ filtered_training_set.csv (includes cage_code column)
├─ stage2_with_tickers.csv
└─ training_set_final.csv
```

### Key Design Decisions

1. **Modular Approach:** Separate `sam_gov_contracts.py` handles all API logic. `build_training_set.py` remains mostly unchanged.
2. **Generator-based:** Yields records one-by-one to avoid buffering entire dataset in memory.
3. **Schema Compatibility:** Normalizes sam.gov fields to match USASpending CSV structure so `build_training_set.py` needs no logic changes.
4. **Checkpoint-based Resume:** Progress saved to `datasets/checkpoints/sam_gov_progress.json` for restart capability.

---

## Component: sam_gov_contracts.py

### Responsibilities

1. **API Authentication**
   - Read `SAM_GOV_API_KEY` from environment
   - Construct authenticated requests to sam.gov API

2. **Pagination & Rate Limiting**
   - sam.gov API uses limit/offset pagination
   - Fetch 1,000 records per request (configurable)
   - Respect sam.gov rate limits (10 req/sec public tier)
   - Sleep between requests to avoid throttling

3. **Field Normalization**
   - Map sam.gov contract fields to USASpending schema:
     - `contractorName` → `awardee_name`
     - `contractAmount` → `award_amount`
     - `cageCode` → **`cage_code`** (new, critical)
     - `effectiveDate` or `awardDate` → `posted_date`
     - `agencyName` → `awarding_agency_name`
     - `subAgencyName` → `awarding_sub_agency_name`
     - `naicsCode` → `naics_code`
     - `contractType` → `type_of_contract_pricing`
     - etc.

4. **Checkpoint Management**
   - File: `datasets/checkpoints/sam_gov_progress.json`
   - State: `{ "last_offset": 45230, "total_fetched": 45230, "timestamp": "2026-03-30T14:23:00" }`
   - On resume: Load last offset, skip to that point in pagination

5. **Error Handling & Retry**
   - Transient errors (429, 503): Retry with exponential backoff (up to 3 times)
   - Permanent errors (401, 404): Fail immediately with diagnostic message
   - Network timeouts: Retry up to 3 times
   - Data validation: Skip records missing required fields, log skips

6. **Logging**
   - Track API response times (detect slowdowns)
   - Log rate-limit hits
   - Summary output: "Fetched X contracts, Y skipped, Z min elapsed"

### Interface

```python
def fetch_contracts(year: int = 2023, api_key: str = None) -> Iterator[dict]:
    """
    Fetch contracts from sam.gov API, paginated and checkpoint-resumable.

    Yields: dict matching USASpending schema + cage_code field
    Raises: RuntimeError if API auth fails or required fields missing
    """
```

### Configuration (in config.py)

```python
# SAM.gov API Configuration
SAM_GOV_API_BASE = "https://api.sam.gov/prod/opendata/"
SAM_GOV_CONTRACT_ENDPOINT = "v1/contracts"
SAM_GOV_RECORDS_PER_PAGE = 1000  # Tune based on API response times
SAM_GOV_RATE_LIMIT_SEC = 0.1  # 10 requests/sec = 0.1s delay
SAM_GOV_RETRY_ATTEMPTS = 3
SAM_GOV_RETRY_BACKOFF_FACTOR = 2.0  # exponential backoff

# API key sourced from environment: os.getenv("SAM_GOV_API_KEY")
# Fail fast if missing
```

---

## Integration with build_training_set.py

### Changes Required (Minimal)

1. **Import the module**
   ```python
   from sam_gov_contracts import fetch_contracts
   ```

2. **Replace data source in `stage1_load_and_filter()`**
   - OLD: `bulk_file = _find_bulk_file(year)` → open zip
   - NEW: `for record in fetch_contracts(year=year):`

3. **Update `_ingest()` logic**
   - Current: reads CSV from zip file
   - New: directly consumes records from generator
   - Schema is identical (USASpending format + CAGE code)

4. **Unchanged**
   - `_parse_bulk_row()` — same filtering logic
   - `stage1_load_and_filter()` — same dedup, IDIQ removal, top-20 removal
   - Stages 2–3 — no changes (TickerResolverV3 already supports CAGE codes)

### Updated Usage

```python
# OLD
python build_training_set.py
# (required: FY2023_All_Contracts_Full_YYYYMMDD.zip in datasets/)

# NEW
export SAM_GOV_API_KEY="your-key-here"
python build_training_set.py
# (required: SAM_GOV_API_KEY environment variable)
```

### Code Delta (Conceptual)

```python
# OLD approach (before):
bulk_file = _find_bulk_file(year)
if not bulk_file:
    log.error("No bulk file found...")
    sys.exit(1)
with zipfile.ZipFile(bulk_file) as zf:
    for csv_name in zf.namelist():
        with zf.open(csv_name) as raw:
            _ingest(raw)

# NEW approach (after):
for record in fetch_contracts(year=year):
    result = _parse_bulk_row(record)
    if result:
        key, parsed = result
        awards_by_key[key] = parsed
```

---

## CAGE Code in Downstream Pipeline

### Stage 2: Ticker Resolution

`build_training_set.py` Stage 2 passes CAGE code to TickerResolverV3:

```python
ticker_result = resolver.resolve(
    awardee_name=award["awardee_name"],
    parent_name=award.get("parent_recipient_name", ""),
    cage_code=award.get("cage_code", "")  # NEW
)
```

TickerResolverV3 now has access to CAGE-based resolution path:
- CAGE → LEI (via CageResolver)
- LEI → Ticker (via LeiResolver)
- Falls back to existing name-based strategies if CAGE fails

### Output Files

- `filtered_training_set.csv` — includes `cage_code` column
- `stage2_with_tickers.csv` — includes `cage_code` (for debugging)
- `training_set_final.csv` — includes `cage_code` (for analysis)

---

## Error Handling & Resilience

### API Failures

| Error | Behavior | Recovery |
|-------|----------|----------|
| 401 Unauthorized | Log error, fail fast | Check API key, re-run with valid key |
| 429 Rate Limited | Sleep + retry (up to 3x) | Automatic, with exponential backoff |
| 503 Service Unavailable | Sleep + retry (up to 3x) | Automatic, retry with backoff |
| Network timeout | Retry (up to 3x) | Automatic, with backoff |
| Connection refused | Fail with diagnostic | Check sam.gov API status, retry later |

### Data Validation

- **Missing CAGE code:** Log warning, continue (CAGE is optional)
- **Missing awardee_name, amount, or date:** Skip record, log count
- **Invalid amount:** Skip record, log error
- **Duplicate records:** Later transaction overwrites earlier (same as USASpending logic)

### Checkpoint Safety

- Checkpoint written after every 50 records processed
- On script restart: load checkpoint, resume from `last_offset`
- Corrupted checkpoint: log error, delete, restart from offset 0
- No duplicate records fetched (offset-based pagination prevents re-fetching)

---

## Testing Strategy

### Unit Tests (`tests/test_sam_gov_contracts.py`)

1. **API Mocking**
   - Mock sam.gov API responses (paginated)
   - Test pagination logic (offset increment, last page detection)
   - Test rate-limit handling (429 response → retry)

2. **Field Normalization**
   - Test sam.gov JSON → USASpending dict mapping
   - Verify all required fields present (awardee_name, amount, date)
   - Verify CAGE code included

3. **Checkpoint Logic**
   - Test checkpoint save/load
   - Test resume from saved offset
   - Test corrupted checkpoint recovery

4. **Error Handling**
   - Test auth errors (fail fast)
   - Test transient errors (retry + backoff)
   - Test validation (skip invalid records)

### Integration Tests

1. **Dry-run against sam.gov** (100–1K records)
   - Fetch real contracts
   - Verify Stage 1 processes them correctly
   - Verify `cage_code` column appears in output
   - Spot-check CAGE codes are valid format

2. **End-to-end flow**
   - Fetch 10K contracts from sam.gov
   - Run through all 3 stages
   - Verify output files created
   - Spot-check ticker resolution with CAGE codes

3. **Performance baseline**
   - Measure API fetch time (1K, 10K, 50K records)
   - Measure Stage 1 processing time
   - Document bottlenecks (API rate limit? Network? Local processing?)

---

## Configuration & Dependencies

### New Dependencies

None — uses existing `requests` library (already in requirements).

### Environment Variables

- **`SAM_GOV_API_KEY`** (required)
  - Your sam.gov API authentication key
  - Set before running: `export SAM_GOV_API_KEY="..."`

### Config Updates (config.py)

```python
# SAM.gov constants
SAM_GOV_API_BASE = "https://api.sam.gov/prod/opendata/"
SAM_GOV_RECORDS_PER_PAGE = 1000
SAM_GOV_RATE_LIMIT_SEC = 0.1  # 10 req/sec
SAM_GOV_RETRY_ATTEMPTS = 3
SAM_GOV_RETRY_BACKOFF_FACTOR = 2.0
```

---

## Implementation Phases

### Phase 1: Core Module (sam_gov_contracts.py)
- Implement API client + pagination
- Implement field normalization
- Implement rate limiting + retry logic
- Unit tests (mocked)

### Phase 2: Integration
- Integrate into `build_training_set.py`
- Update documentation
- Test with real sam.gov API (dry-run 100 records)

### Phase 3: Full Run & Validation
- Fetch full contract set (all records for year)
- Run through all 3 stages
- Verify CAGE codes in output
- Spot-check ticker resolution improvements
- Performance profiling

---

## Success Criteria

1. ✅ `sam_gov_contracts.py` fetches contracts via sam.gov API
2. ✅ CAGE codes included in `filtered_training_set.csv`
3. ✅ `build_training_set.py` requires zero logic changes (only data source swap)
4. ✅ Checkpoint-based resume works (tested by interrupting and restarting)
5. ✅ Unit tests pass (mocked API)
6. ✅ Integration test passes (real sam.gov API, 100 records)
7. ✅ Full run completes without errors
8. ✅ TickerResolverV3 receives CAGE codes and uses them in resolution

---

## Notes

- sam.gov API docs: https://open.gsa.gov/api/
- CAGE code format: 5-character alphanumeric (e.g., "19364")
- TickerResolverV3 already supports CAGE codes — no changes needed there
- Token efficiency: Use RTK prefix on all commands per CLAUDE.md
