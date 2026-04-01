# SAM.gov Bulk CSV Reader + TickerResolverV4 Design

**Date:** 2026-03-31
**Status:** Design Approved
**Objective:** Replace USASpending ZIP-based data source with SAM.gov bulk CSV export, and rebuild the ticker resolver (V4) to exploit all new data fields for maximum resolution accuracy.

---

## Problem Statement

The previous design (2026-03-30-sam-gov-data-source-design.md) proposed fetching contracts via the SAM.gov API one page at a time. A bulk CSV export from SAM.gov is now available (`FirstReport.csv`), which is faster, simpler (no API key required for the data itself), and contains a richer schema than USASpending — including CAGE codes, multiple name fields, UEI, country of incorporation, and 40+ vendor business-type flags.

**Gap closed:**
- Old USASpending CSV: no CAGE codes, one name field, no non-public flags
- New SAM.gov CSV: CAGE code, 4 name fields, UEI, country, 6+ non-public flags

---

## Approach Selected: Modular Reader + Clean V4

Three approaches were evaluated:

| Approach | Description | Decision |
|---|---|---|
| A — Inline remap | Edit `_parse_bulk_row()` column names in-place | Rejected — mixes schema knowledge into builder |
| **B — Reader module + V4** | `sam_gov_reader.py` yields typed records; V4 is clean new class | **Selected** |
| C — Full rewrite | Redesign pipeline from scratch | Rejected — high risk, discards working code |

---

## Architecture

```
datasets/FirstReport.csv   (SAM.gov bulk export, drop new exports here)
        ↓
sam_gov_reader.py
├─ read_sam_gov_csv(path) → Iterator[ContractRecord]
├─ Hard-reject: foreign entities, IDV umbrellas, out-of-range amounts
├─ Parse business-type flags ("Yes"/"No" → bool)
└─ Yield typed ContractRecord dataclasses
        ↓
build_training_set.py (Stage 1 only — swap reader, add _record_to_award_dict)
├─ Stage 1: _find_sam_gov_csv() + read_sam_gov_csv() → awards_by_key
├─ Stage 2: TickerResolverV4.resolve(record) [ContractRecord attached to award]
└─ Stage 3: Enrich — unchanged
        ↓
ticker_resolver_v4.py
├─ Tier 0: Hard rejects (country, business-type flags, name regex)
├─ Tier 1: CAGE → GLEIF → LEI → OpenFIGI
├─ Tier 2: Multi-name EDGAR exact match (4 names)
├─ Tier 3: Multi-name EDGAR fuzzy match (4 names, threshold 85)
├─ Tier 4: Substring match (subsidiary catch)
└─ Tier 5: Sole-source tag (num_offers == "1" → tagged for scorer)
```

---

## Data Contract: ContractRecord

Defined in `sam_gov_reader.py`. Passed from reader → builder → resolver.

```python
@dataclass
class ContractRecord:
    # Identity
    piid: str                        # PIID (unique contract ID, replaces contract_award_unique_key)
    cage_code: str                   # CAGE Code
    uei: str                         # Unique Entity ID (SAM.gov identifier, post-DUNS)
    country_of_incorporation: str    # "USA" → keep; anything else → hard reject

    # Names (resolution attempt order: legal → contractor → dba → parent)
    contractor_name: str             # Contractor Name
    legal_business_name: str         # Legal Business Name (most authoritative)
    dba_name: str                    # Doing Business As Name
    parent_name: str                 # Ultimate Parent Legal Business Name
    parent_uei: str                  # Ultimate Parent Unique Entity ID

    # Contract fields
    award_amount: float              # Base and All Options Value (Total Contract Value)
    posted_date: str                 # Period of Performance Start Date (YYYY-MM-DD)
    agency: str                      # Contracting Agency Name
    naics_code: str                  # NAICS Code
    naics_description: str           # NAICS Description
    set_aside_code: str              # Type of Set Aside Code
    extent_competed_code: str        # Extent Competed Code
    other_than_full_open: str        # Other Than Full and Open Competition Code
    idv_type: str                    # IDV Type (non-empty = IDV umbrella → filter out)
    num_offers: str                  # Number of Offers Received

    # Non-public detection flags (from "Is Vendor Business Type - X" columns, "Yes"/"No" → bool)
    is_educational_institution: bool
    is_federal_agency: bool
    is_airport_authority: bool
    is_council_of_governments: bool
    is_community_dev_corp: bool
    is_federally_funded_rd: bool     # Federally Funded Research and Development Corporation
```

**Award amount field:** `Base and All Options Value (Total Contract Value)` — represents full contract scope including options, consistent with the previous `current_total_value_of_award` from USASpending.

---

## Component 1: sam_gov_reader.py

### Responsibilities

1. Open CSV with `csv.DictReader` — row-by-row, no full load into memory
2. Hard-reject foreign entities (`Country of Incorporation != "USA"`) — silently skipped (high volume)
3. Parse and validate `Base and All Options Value (Total Contract Value)` — skip if unparseable
4. Apply range filter: `MIN_CONTRACT_VALUE ≤ amount ≤ MAX_AWARD_AMOUNT`
5. Filter IDV umbrella contracts: `IDV Type` non-empty → skip
6. Parse 6 business-type boolean flags from `"Yes"/"No"` string columns
7. Yield `ContractRecord` for each valid row

### Interface

```python
def read_sam_gov_csv(path: str) -> Iterator[ContractRecord]:
    """Read SAM.gov bulk CSV export, yield validated ContractRecord per row.

    Silently skips: foreign entities, IDV umbrellas, out-of-range amounts.
    Raises: FileNotFoundError if path does not exist.
    """
```

### CSV detection in build_training_set.py

```python
def _find_sam_gov_csv() -> str:
    """Scan datasets/ for SAM.gov bulk CSV exports, return most recent."""
    patterns = [
        os.path.join(DATASET_DIR, "*Report*.csv"),
        os.path.join(DATASET_DIR, "*SAM*.csv"),
    ]
```

No checkpointing in the reader — local CSV reads are fast. Checkpointing remains in Stage 1 of `build_training_set.py` as before.

---

## Component 2: TickerResolverV4

### Resolution Pipeline (ordered, stops at first hit)

#### Tier 0 — Hard Rejects (zero API calls)

Immediately return `non_public_entity` if any of:
- `country_of_incorporation != "USA"` (should not reach V4 after reader filters, but defensive)
- Any of the 6 business-type flags is `True`
- Name matches V3 regex patterns: universities, `COUNTY OF`, `STATE OF`, `DEPARTMENT OF`, `FOUNDATION`, `AUTHORITY`, joint ventures, national labs

**Cost:** Zero API calls. Fast path for the majority of non-public contractors.

#### Tier 1 — CAGE → GLEIF → LEI → OpenFIGI

If `cage_code` is valid (5 alphanumeric chars):
- `CageResolver.resolve_cage(cage_code)` → LEI
- `LeiResolver.resolve_lei(lei)` → ticker

Reuses existing `CageResolver` and `LeiResolver` from V3. Structured identifier chain — no fuzzy matching, highest fidelity.

**Confidence:** `high` | **Evidence:** `cage_lei_openfigi`

#### Tier 2 — Multi-name EDGAR Exact Match

Try each name in priority order:
1. `legal_business_name` (most authoritative per SAM.gov)
2. `contractor_name`
3. `dba_name`
4. `parent_name`

For each: normalize → strip suffixes → exact lookup in EDGAR map → if hit, validate via SEC submissions CIK. First validated hit wins.

**Why V4 improves on V3:** V3 tried one name. Four attempts with legal name first dramatically increases exact-match hit rate.

**Confidence:** `high` (exact + validated) | `medium` (exact, unverified CIK)

#### Tier 3 — Multi-name EDGAR Fuzzy Match

Same four names, same order. `process.extract()` with `token_sort_ratio`, threshold 85 (80 for names ≤3 words). Top 5 candidates per name get SEC submission validation. First validated hit across all names wins.

**Confidence:** `medium_high` (fuzzy + validated)

#### Tier 4 — Substring Match (subsidiary catch)

Applied to `contractor_name` and `legal_business_name`. Catches cases like `"NORTHROP GRUMMAN SYSTEMS CORP"` → `"NORTHROP GRUMMAN"`. Requires ≥60% overlap, ≥10 char match. Validates ticker has market cap > 0.

**Confidence:** `medium` | **Evidence:** `substring_match`

#### Tier 5 — Sole-source Tag

If `num_offers == "1"` (or `extent_competed_code` indicates not competed) and still unresolved: tag `rejection_reason = "sole_source_unresolved"`. The scoring engine can assign sole-source points without a ticker.

### Confidence Table

| Tier | Evidence type | Confidence |
|---|---|---|
| 1 | `cage_lei_openfigi` | `high` |
| 2 | `exact_sec_name` | `high` |
| 2 | `former_name_exact` | `medium_high` |
| 2 | `exact_edgar_map_unverified` | `medium` |
| 3 | `fuzzy_exact_sec_name` | `medium_high` |
| 3 | `fuzzy_former_name` | `medium` |
| 4 | `substring_match` | `medium` |

### Interface

```python
class TickerResolverV4:
    def resolve(self, record: ContractRecord) -> dict:
        """Resolve a ContractRecord to ticker/CIK.

        Returns dict with keys: resolved_ticker, resolved_cik, evidence_type,
        confidence, rejection_reason, market_cap_current, audit_trail.
        """
```

Cache key: `record.cage_code or record.uei or record.legal_business_name or record.contractor_name` — prefer stable identifiers over mutable names. Cache file: `.ticker_cache_v4.json` (separate from V3's `.ticker_cache_v2.json` to avoid cross-version collisions).

---

## Component 3: build_training_set.py Changes

### Stage 1 — Load & Filter

**Removed:**
- `_find_bulk_file()` — ZIP scanner
- `_parse_bulk_row()` — USASpending column parser
- `zipfile` import

**Added:**
- `_find_sam_gov_csv()` — scans `datasets/` for SAM.gov CSV exports
- `_record_to_award_dict(record: ContractRecord) -> dict` — maps typed record to existing award dict schema (15-line function, same keys Stage 2/3 expect)
- Import: `from sam_gov_reader import read_sam_gov_csv, ContractRecord`

**Stage 1 loop (conceptual delta):**
```python
# OLD
with zipfile.ZipFile(bulk_file) as zf:
    for row in csv.DictReader(raw):
        result = _parse_bulk_row(row)

# NEW
for record in read_sam_gov_csv(sam_csv):
    awards_by_key[record.piid] = _record_to_award_dict(record)
```

### Stage 2 — Ticker Resolve

- Replace `TickerResolverV3` with `TickerResolverV4`
- Award dicts get `_record: ContractRecord` attached (not written to CSV) so V4 has full context
- Call: `resolver.resolve(award["_record"])`

### Stage 3 — Enrich

Unchanged.

### New output columns in filtered_training_set.csv

| Column | Source |
|---|---|
| `cage_code` | `ContractRecord.cage_code` |
| `uei` | `ContractRecord.uei` |
| `legal_business_name` | `ContractRecord.legal_business_name` |
| `dba_name` | `ContractRecord.dba_name` |
| `country_of_incorporation` | `ContractRecord.country_of_incorporation` |

---

## CSV File Placement

- **Location:** `datasets/FirstReport.csv`
- **Detection:** `_find_sam_gov_csv()` scans `datasets/` for `*Report*.csv` or `*SAM*.csv`, takes the most recently modified file
- **Workflow:** Drop new SAM.gov exports into `datasets/` and re-run — auto-detected

---

## Error Handling

| Scenario | Behavior |
|---|---|
| `award_amount` unparseable | Skip row silently |
| `Country of Incorporation` missing | Default to `""` → hard reject (not `"USA"`) |
| `CAGE Code` invalid format | Skip Tier 1, continue to Tier 2 |
| GLEIF API timeout | Log warning, continue to Tier 2 |
| EDGAR map missing | `TickerResolverV4.__init__` loads/downloads on startup |
| CSV file not found | `_find_sam_gov_csv()` raises `RuntimeError` with clear message |

---

## Testing Strategy

### Unit tests (new)

1. `tests/test_sam_gov_reader.py`
   - Parse valid row → correct `ContractRecord` fields
   - Foreign entity row → skipped
   - IDV row → skipped
   - Out-of-range amount → skipped
   - Business-type flags `"Yes"/"No"` → `True/False`

2. `tests/test_ticker_resolver_v4.py`
   - Tier 0: each non-public flag → `non_public_entity`
   - Tier 1: valid CAGE → mocked GLEIF/OpenFIGI → ticker
   - Tier 2: exact legal name match → ticker
   - Tier 2: DBA name fallback → ticker
   - Tier 3: fuzzy name match → ticker
   - Tier 4: subsidiary substring → parent ticker
   - Cache key uses `cage_code` over name when available

### Integration smoke test

Run Stage 1 only against `FirstReport.csv`, verify:
- Output row count reasonable
- `cage_code` column populated for majority of rows
- No IDV rows in output
- All `country_of_incorporation` values are `"USA"`

---

## Success Criteria

1. `sam_gov_reader.py` reads `FirstReport.csv` and yields `ContractRecord` objects
2. `build_training_set.py` Stage 1 uses `read_sam_gov_csv()` instead of ZIP reader
3. `TickerResolverV4` resolves `ContractRecord` with 4-name multi-tier pipeline
4. `filtered_training_set.csv` includes `cage_code`, `uei`, `legal_business_name` columns
5. CAGE-based resolution path (Tier 1) activates for rows with valid CAGE codes
6. Foreign entity rows hard-rejected in reader (not reaching resolver)
7. Unit tests pass for reader and V4
8. Stage 1 smoke test completes against `FirstReport.csv`
