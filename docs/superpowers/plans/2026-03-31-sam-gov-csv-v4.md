# SAM.gov Bulk CSV + TickerResolverV4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the USASpending ZIP-based data source with a SAM.gov bulk CSV export and build TickerResolverV4 — a clean resolver that exploits CAGE codes, four name fields, business-type flags, and country of incorporation for dramatically better ticker resolution accuracy.

**Architecture:** `sam_gov_reader.py` reads the SAM.gov CSV and yields typed `ContractRecord` dataclasses, hard-rejecting foreign entities and IDV umbrellas at read time. `build_training_set.py` Stage 1 swaps to the new reader (minimal change); Stage 2 swaps to `TickerResolverV4` which resolves via CAGE→GLEIF→LEI→OpenFIGI (Tier 1), then multi-name EDGAR exact/fuzzy/substring matching (Tiers 2–4). Stage 3 is untouched.

**Tech Stack:** Python 3.12, `csv`, `dataclasses`, `rapidfuzz`, `requests`, `yfinance`, `sec-cik-mapper`, `pytest`, `unittest.mock`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `datasets/FirstReport.csv` | Add | SAM.gov bulk CSV export (source data) |
| `sam_gov_reader.py` | Create | `ContractRecord` dataclass + `read_sam_gov_csv()` iterator |
| `tests/test_sam_gov_reader.py` | Create | Unit tests for reader |
| `ticker_resolver_v4.py` | Create | `TickerResolverV4` — 5-tier resolution pipeline |
| `tests/test_ticker_resolver_v4.py` | Create | Unit tests for V4 |
| `build_training_set.py` | Modify | Stage 1: swap reader; Stage 2: swap resolver |

---

## Task 1: Place SAM.gov CSV in Project

**Files:**
- Add: `datasets/FirstReport.csv`

- [ ] **Step 1: Create datasets directory if missing and copy CSV**

```bash
mkdir -p datasets
cp "/c/Users/decla/Downloads/FirstReport (1).csv" datasets/FirstReport.csv
```

- [ ] **Step 2: Verify file is readable**

```bash
rtk python -c "
import csv
with open('datasets/FirstReport.csv', newline='', encoding='utf-8') as f:
    r = csv.DictReader(f)
    row = next(r)
    print('Columns:', len(r.fieldnames))
    print('CAGE Code sample:', row.get('CAGE Code', 'MISSING'))
    print('Country sample:', row.get('Country of Incorporation', 'MISSING'))
"
```

Expected: prints column count, a CAGE code value, and country.

- [ ] **Step 3: Add datasets/ CSV files to .gitignore (data files should not be committed)**

Check `.gitignore`. If `datasets/*.csv` is not already ignored, note that the CSV is large and should not be committed. Only commit the directory structure (the CSV itself stays local).

- [ ] **Step 4: Commit directory marker**

```bash
rtk git add datasets/FirstReport.csv
rtk git commit -m "data: Add SAM.gov bulk CSV export to datasets/"
```

> Note: If the file is too large for git or already gitignored, skip the commit — just verify it exists locally.

---

## Task 2: Create sam_gov_reader.py (TDD)

**Files:**
- Create: `sam_gov_reader.py`
- Create: `tests/test_sam_gov_reader.py`

### Step 2a: Write the failing tests first

- [ ] **Step 1: Create `tests/test_sam_gov_reader.py`**

```python
"""Unit tests for sam_gov_reader.py"""
import csv
import io
import os
import tempfile
import pytest
from sam_gov_reader import ContractRecord, read_sam_gov_csv


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_csv_file(rows: list[dict]) -> str:
    """Write rows to a temp CSV file, return path."""
    if not rows:
        raise ValueError("rows must be non-empty")
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return f.name


def _base_row(**overrides) -> dict:
    """Return a minimal valid SAM.gov CSV row dict."""
    row = {
        "PIID": "W911NF23C1001",
        "CAGE Code": "1RBX4",
        "Unique Entity ID": "ABCDEF123456",
        "Contractor Name": "ACME DEFENSE INC",
        "Legal Business Name": "ACME DEFENSE INC",
        "Doing Business As Name": "",
        "Ultimate Parent Legal Business Name": "ACME CORP",
        "Ultimate Parent Unique Entity ID": "PARENT123456",
        "Country of Incorporation": "USA",
        "Base and All Options Value (Total Contract Value)": "5000000.00",
        "Period of Performance Start Date": "2023-03-15",
        "Contracting Agency Name": "DEPT OF DEFENSE",
        "NAICS Code": "336411",
        "NAICS Description": "Aircraft Manufacturing",
        "Type of Set Aside Code": "",
        "Extent Competed Code": "A",
        "Other Than Full and Open Competition Code": "",
        "IDV Type": "",
        "Number of Offers Received": "3",
        "Is Vendor Business Type - Educational Institution": "No",
        "Is Vendor Business Type - Federal Agency": "No",
        "Is Vendor Business Type - Airport Authority": "No",
        "Is Vendor Business Type - Council Of Governments": "No",
        "Is Vendor Business Type - Community Development Corporation": "No",
        "Is Vendor Business Type - Federally Funded Research and Development Corporation": "No",
    }
    row.update(overrides)
    return row


# ─── Tests ───────────────────────────────────────────────────────────────────

def test_valid_row_yields_contract_record():
    path = _make_csv_file([_base_row()])
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 1
        r = records[0]
        assert isinstance(r, ContractRecord)
        assert r.piid == "W911NF23C1001"
        assert r.cage_code == "1RBX4"
        assert r.uei == "ABCDEF123456"
        assert r.contractor_name == "ACME DEFENSE INC"
        assert r.legal_business_name == "ACME DEFENSE INC"
        assert r.dba_name == ""
        assert r.parent_name == "ACME CORP"
        assert r.parent_uei == "PARENT123456"
        assert r.country_of_incorporation == "USA"
        assert r.award_amount == 5_000_000.0
        assert r.posted_date == "2023-03-15"
        assert r.agency == "DEPT OF DEFENSE"
        assert r.naics_code == "336411"
        assert r.set_aside_code == ""
        assert r.extent_competed_code == "A"
        assert r.idv_type == ""
        assert r.num_offers == "3"
        assert r.is_educational_institution is False
        assert r.is_federal_agency is False
    finally:
        os.unlink(path)


def test_foreign_entity_skipped():
    path = _make_csv_file([_base_row(**{"Country of Incorporation": "GBR"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_missing_country_skipped():
    path = _make_csv_file([_base_row(**{"Country of Incorporation": ""})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_idv_type_nonempty_skipped():
    path = _make_csv_file([_base_row(**{"IDV Type": "IDC"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_amount_below_min_skipped():
    # MIN_CONTRACT_VALUE is $1M
    path = _make_csv_file([_base_row(**{"Base and All Options Value (Total Contract Value)": "500000"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_amount_above_max_skipped():
    # MAX_AWARD_AMOUNT is $10B
    path = _make_csv_file([_base_row(**{"Base and All Options Value (Total Contract Value)": "15000000000"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_unparseable_amount_skipped():
    path = _make_csv_file([_base_row(**{"Base and All Options Value (Total Contract Value)": "N/A"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_business_type_flags_yes_parsed_as_true():
    path = _make_csv_file([_base_row(**{
        "Is Vendor Business Type - Educational Institution": "Yes",
        "Is Vendor Business Type - Federal Agency": "Yes",
    })])
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 1
        assert records[0].is_educational_institution is True
        assert records[0].is_federal_agency is True
    finally:
        os.unlink(path)


def test_multiple_rows_mixed_filtering():
    rows = [
        _base_row(PIID="A001"),                                        # valid
        _base_row(PIID="A002", **{"Country of Incorporation": "CAN"}),  # foreign → skip
        _base_row(PIID="A003", **{"IDV Type": "IDC"}),                  # IDV → skip
        _base_row(PIID="A004"),                                        # valid
    ]
    path = _make_csv_file(rows)
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 2
        assert records[0].piid == "A001"
        assert records[1].piid == "A004"
    finally:
        os.unlink(path)


def test_file_not_found_raises():
    with pytest.raises(FileNotFoundError):
        list(read_sam_gov_csv("nonexistent_file.csv"))
```

- [ ] **Step 2: Run tests to confirm they all fail (module doesn't exist yet)**

```bash
rtk python -m pytest tests/test_sam_gov_reader.py -v
```

Expected: `ModuleNotFoundError: No module named 'sam_gov_reader'`

### Step 2b: Implement sam_gov_reader.py

- [ ] **Step 3: Create `sam_gov_reader.py`**

```python
"""SAM.gov bulk CSV reader — yields typed ContractRecord dataclasses.

Filters applied at read time (no downstream changes needed):
- Hard-reject: Country of Incorporation != "USA"
- Hard-reject: IDV Type non-empty (umbrella contract vehicles)
- Hard-reject: award amount outside $1M–$10B range
- Hard-reject: unparseable award amount
"""
from __future__ import annotations

import csv
import glob
import os
from dataclasses import dataclass
from typing import Iterator

from config import MIN_CONTRACT_VALUE, MAX_AWARD_AMOUNT

_AMOUNT_COL = "Base and All Options Value (Total Contract Value)"

_FLAG_COLS = {
    "is_educational_institution": "Is Vendor Business Type - Educational Institution",
    "is_federal_agency":          "Is Vendor Business Type - Federal Agency",
    "is_airport_authority":       "Is Vendor Business Type - Airport Authority",
    "is_council_of_governments":  "Is Vendor Business Type - Council Of Governments",
    "is_community_dev_corp":      "Is Vendor Business Type - Community Development Corporation",
    "is_federally_funded_rd":     "Is Vendor Business Type - Federally Funded Research and Development Corporation",
}


@dataclass
class ContractRecord:
    # Identity
    piid: str
    cage_code: str
    uei: str
    country_of_incorporation: str

    # Names (priority order for resolution: legal → contractor → dba → parent)
    contractor_name: str
    legal_business_name: str
    dba_name: str
    parent_name: str
    parent_uei: str

    # Contract fields
    award_amount: float
    posted_date: str
    agency: str
    naics_code: str
    naics_description: str
    set_aside_code: str
    extent_competed_code: str
    other_than_full_open: str
    idv_type: str
    num_offers: str

    # Non-public detection flags
    is_educational_institution: bool
    is_federal_agency: bool
    is_airport_authority: bool
    is_council_of_governments: bool
    is_community_dev_corp: bool
    is_federally_funded_rd: bool


def read_sam_gov_csv(path: str) -> Iterator[ContractRecord]:
    """Read SAM.gov bulk CSV export, yield validated ContractRecord per row.

    Silently skips: foreign entities, IDV umbrellas, out-of-range/unparseable amounts.
    Raises: FileNotFoundError if path does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"SAM.gov CSV not found: {path}")

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Hard-reject: foreign / missing country
            country = (row.get("Country of Incorporation") or "").strip().upper()
            if country != "USA":
                continue

            # Hard-reject: IDV umbrella contracts
            if (row.get("IDV Type") or "").strip():
                continue

            # Hard-reject: unparseable or out-of-range amount
            try:
                amount = float((row.get(_AMOUNT_COL) or "0").replace(",", ""))
            except (ValueError, TypeError):
                continue
            if amount < MIN_CONTRACT_VALUE or amount > MAX_AWARD_AMOUNT:
                continue

            yield ContractRecord(
                piid=                   (row.get("PIID") or "").strip(),
                cage_code=              (row.get("CAGE Code") or "").strip(),
                uei=                    (row.get("Unique Entity ID") or "").strip(),
                country_of_incorporation=country,
                contractor_name=        (row.get("Contractor Name") or "").strip(),
                legal_business_name=    (row.get("Legal Business Name") or "").strip(),
                dba_name=               (row.get("Doing Business As Name") or "").strip(),
                parent_name=            (row.get("Ultimate Parent Legal Business Name") or "").strip(),
                parent_uei=             (row.get("Ultimate Parent Unique Entity ID") or "").strip(),
                award_amount=           amount,
                posted_date=            _parse_date(row.get("Period of Performance Start Date") or ""),
                agency=                 (row.get("Contracting Agency Name") or "").strip(),
                naics_code=             (row.get("NAICS Code") or "").strip(),
                naics_description=      (row.get("NAICS Description") or "").strip(),
                set_aside_code=         (row.get("Type of Set Aside Code") or "").strip(),
                extent_competed_code=   (row.get("Extent Competed Code") or "").strip(),
                other_than_full_open=   (row.get("Other Than Full and Open Competition Code") or "").strip(),
                idv_type=               (row.get("IDV Type") or "").strip(),
                num_offers=             (row.get("Number of Offers Received") or "").strip(),
                is_educational_institution= _yes(row, _FLAG_COLS["is_educational_institution"]),
                is_federal_agency=          _yes(row, _FLAG_COLS["is_federal_agency"]),
                is_airport_authority=       _yes(row, _FLAG_COLS["is_airport_authority"]),
                is_council_of_governments=  _yes(row, _FLAG_COLS["is_council_of_governments"]),
                is_community_dev_corp=      _yes(row, _FLAG_COLS["is_community_dev_corp"]),
                is_federally_funded_rd=     _yes(row, _FLAG_COLS["is_federally_funded_rd"]),
            )


def _yes(row: dict, col: str) -> bool:
    return (row.get(col) or "").strip().upper() == "YES"


def _parse_date(raw: str) -> str:
    """Return first 10 chars (YYYY-MM-DD) or empty string."""
    raw = raw.strip()
    return raw[:10] if len(raw) >= 10 else raw


def find_sam_gov_csv(dataset_dir: str) -> str:
    """Scan dataset_dir for SAM.gov bulk CSV exports, return path of most recent.

    Raises: RuntimeError if no matching CSV found.
    """
    patterns = [
        os.path.join(dataset_dir, "*Report*.csv"),
        os.path.join(dataset_dir, "*SAM*.csv"),
    ]
    matches = []
    for pat in patterns:
        matches.extend(glob.glob(pat))
    if not matches:
        raise RuntimeError(
            f"No SAM.gov CSV found in {dataset_dir}/\n"
            "Download a report from sam.gov and place it in the datasets/ directory."
        )
    # Most recently modified file wins
    return max(matches, key=os.path.getmtime)
```

- [ ] **Step 4: Run tests — all should pass**

```bash
rtk python -m pytest tests/test_sam_gov_reader.py -v
```

Expected: all 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add sam_gov_reader.py tests/test_sam_gov_reader.py
rtk git commit -m "feat: Add sam_gov_reader with ContractRecord dataclass and tests"
```

---

## Task 3: Create ticker_resolver_v4.py — Scaffold + Tier 0

**Files:**
- Create: `ticker_resolver_v4.py`
- Create: `tests/test_ticker_resolver_v4.py`

- [ ] **Step 1: Create `tests/test_ticker_resolver_v4.py` with Tier 0 tests**

```python
"""Unit tests for TickerResolverV4."""
import pytest
from unittest.mock import MagicMock, patch
from sam_gov_reader import ContractRecord
from ticker_resolver_v4 import TickerResolverV4


# ─── Factory ─────────────────────────────────────────────────────────────────

def make_record(**overrides) -> ContractRecord:
    defaults = dict(
        piid="W911NF23C1001",
        cage_code="1RBX4",
        uei="ABCDEF123456",
        country_of_incorporation="USA",
        contractor_name="ACME DEFENSE INC",
        legal_business_name="ACME DEFENSE INC",
        dba_name="",
        parent_name="ACME CORP",
        parent_uei="PARENT123456",
        award_amount=5_000_000.0,
        posted_date="2023-03-15",
        agency="DEPT OF DEFENSE",
        naics_code="336411",
        naics_description="Aircraft Manufacturing",
        set_aside_code="",
        extent_competed_code="A",
        other_than_full_open="",
        idv_type="",
        num_offers="3",
        is_educational_institution=False,
        is_federal_agency=False,
        is_airport_authority=False,
        is_council_of_governments=False,
        is_community_dev_corp=False,
        is_federally_funded_rd=False,
    )
    defaults.update(overrides)
    return ContractRecord(**defaults)


def make_resolver(edgar_map: dict | None = None) -> TickerResolverV4:
    """Create resolver with empty EDGAR map (no network calls)."""
    return TickerResolverV4(edgar_map=edgar_map or {}, cache_path=":memory:")


# ─── Tier 0 tests ─────────────────────────────────────────────────────────────

def test_tier0_educational_institution_rejected():
    r = make_resolver()
    record = make_record(is_educational_institution=True)
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_federal_agency_rejected():
    r = make_resolver()
    record = make_record(is_federal_agency=True)
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_airport_authority_rejected():
    r = make_resolver()
    record = make_record(is_airport_authority=True)
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_council_of_governments_rejected():
    r = make_resolver()
    record = make_record(is_council_of_governments=True)
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_community_dev_corp_rejected():
    r = make_resolver()
    record = make_record(is_community_dev_corp=True)
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_federally_funded_rd_rejected():
    r = make_resolver()
    record = make_record(is_federally_funded_rd=True)
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_name_regex_university_rejected():
    r = make_resolver()
    record = make_record(contractor_name="UNIVERSITY OF MICHIGAN", legal_business_name="UNIVERSITY OF MICHIGAN")
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_name_regex_county_rejected():
    r = make_resolver()
    record = make_record(contractor_name="COUNTY OF MONTGOMERY", legal_business_name="COUNTY OF MONTGOMERY")
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_foreign_country_rejected():
    r = make_resolver()
    record = make_record(country_of_incorporation="GBR")
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_clean_record_not_rejected():
    r = make_resolver()
    record = make_record()  # all flags False, country USA, normal name
    result = r.resolve(record)
    # Should fall through to unresolved (no EDGAR map entries), NOT non_public_entity
    assert result["rejection_reason"] != "non_public_entity"
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
rtk python -m pytest tests/test_ticker_resolver_v4.py -v
```

Expected: `ModuleNotFoundError: No module named 'ticker_resolver_v4'`

- [ ] **Step 3: Create `ticker_resolver_v4.py` with scaffold and Tier 0**

```python
"""TickerResolverV4 — multi-tier resolver using SAM.gov ContractRecord fields.

Resolution pipeline (stops at first hit):
  Tier 0: Hard rejects (non-public flags, foreign country, name regex)
  Tier 1: CAGE → GLEIF → LEI → OpenFIGI
  Tier 2: Multi-name EDGAR exact match (4 names, priority order)
  Tier 3: Multi-name EDGAR fuzzy match (4 names, threshold 85)
  Tier 4: Substring match (catch subsidiaries)
  Tier 5: Sole-source tag (num_offers == "1" → tag for scorer)
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional

import requests
import yfinance as yf
from rapidfuzz import fuzz, process

from config import EDGAR_RATE_LIMIT, EDGAR_USER_AGENT
from cage_resolver import CageResolver
from lei_resolver import LeiResolver
from sam_gov_reader import ContractRecord

# Reuse shared EDGAR utilities from V3
from ticker_resolver_v3 import (
    _load_edgar_map_default,
    _normalize,
    _strip_suffixes,
    _validate_candidate,
    _edgar_throttle,
    _NON_PUBLIC_RE,
    EDGAR_SUBMISSIONS_URL,
    EDGAR_HEADERS,
)

log = logging.getLogger(__name__)

_MCAP_CACHE_WRITE_BATCH = 50

# Extent Competed codes that indicate no competition / sole source
_NOT_COMPETED_CODES = {"B", "C", "G", "CDO", "URG", "SP2", "FOLLOW ON"}


class TickerResolverV4:
    """5-tier resolver operating on ContractRecord from sam_gov_reader."""

    def __init__(self, edgar_map: dict | None = None,
                 cache_path: str = ".ticker_cache_v4.json",
                 mcap_cache_path: str = ".mcap_cache.json"):
        if edgar_map is None:
            edgar_map = _load_edgar_map_default()
        self.edgar_map = edgar_map
        self.cache_path = cache_path
        self.mcap_cache_path = mcap_cache_path
        self.cache: dict = {}
        self.mcap_cache: dict = {}
        self._mcap_unsaved = 0
        self.cage_resolver = CageResolver()
        self.lei_resolver = LeiResolver()

        if cache_path != ":memory:":
            self._load_cache()
            self._load_mcap_cache()

        # Pre-build EDGAR lookup indices
        self._stripped_map: dict[str, tuple[str, dict]] = {}
        self._edgar_names: list[str] = list(edgar_map.keys())
        self._substr_candidates: list[tuple[str, str, dict]] = []
        for ename, entry in edgar_map.items():
            s = _strip_suffixes(_normalize(ename))
            if s and s not in self._stripped_map:
                self._stripped_map[s] = (ename, entry)
            if s and len(s.split()) >= 2:
                self._substr_candidates.append((s, ename, entry))

    # ── Cache I/O ────────────────────────────────────────────────────────────

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path) as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {}

    def _load_mcap_cache(self):
        if os.path.exists(self.mcap_cache_path):
            try:
                with open(self.mcap_cache_path) as f:
                    self.mcap_cache = json.load(f)
            except Exception:
                self.mcap_cache = {}

    def save_cache(self):
        if self.cache_path == ":memory:":
            return
        with open(self.cache_path, "w") as f:
            json.dump(self.cache, f, indent=2)
        if self._mcap_unsaved > 0:
            self._flush_mcap_cache()

    def _flush_mcap_cache(self):
        if self.mcap_cache_path == ":memory:":
            return
        with open(self.mcap_cache_path, "w") as f:
            json.dump(self.mcap_cache, f, indent=2)
        self._mcap_unsaved = 0

    # ── Public API ────────────────────────────────────────────────────────────

    def resolve(self, record: ContractRecord) -> dict:
        """Resolve a ContractRecord → ticker result dict.

        Cache key prefers stable identifiers (CAGE, UEI) over mutable names.
        """
        cache_key = (record.cage_code or record.uei or
                     record.legal_business_name or record.contractor_name)
        if cache_key and cache_key in self.cache:
            return self.cache[cache_key]

        result = self._resolve(record)

        if cache_key:
            self.cache[cache_key] = result
        return result

    # ── Resolution pipeline ───────────────────────────────────────────────────

    def _resolve(self, record: ContractRecord) -> dict:
        primary_name = record.contractor_name or record.legal_business_name

        # Tier 0: hard rejects
        if self._is_non_public(record):
            return self._make_result(primary_name, _normalize(primary_name),
                                     None, None, "none", "unresolved", "non_public_entity")

        # Tier 1: CAGE → GLEIF → LEI → OpenFIGI
        if record.cage_code:
            r = self._resolve_via_cage(record)
            if r.get("resolved_ticker"):
                return r

        # Names to try in Tiers 2–4 (skip empty)
        names = [n for n in [
            record.legal_business_name,
            record.contractor_name,
            record.dba_name,
            record.parent_name,
        ] if n.strip()]

        # Tier 2: multi-name exact match
        for name in names:
            r = self._exact_match(name)
            if r:
                return r

        # Tier 3: multi-name fuzzy match
        for name in names:
            r = self._fuzzy_match(name)
            if r:
                return r

        # Tier 4: substring match (subsidiary catch)
        for name in [record.contractor_name, record.legal_business_name]:
            if name.strip():
                r = self._substring_match(name)
                if r:
                    return r

        # Tier 5: sole-source tag
        rejection = "no_match"
        if (record.num_offers == "1" or
                record.extent_competed_code.upper() in _NOT_COMPETED_CODES or
                record.other_than_full_open.strip()):
            rejection = "sole_source_unresolved"

        return self._make_result(primary_name, _normalize(primary_name),
                                  None, None, "none", "unresolved", rejection)

    # ── Tier 0: non-public detection ─────────────────────────────────────────

    def _is_non_public(self, record: ContractRecord) -> bool:
        if record.country_of_incorporation.upper() not in ("USA", ""):
            # Allow empty (reader already filtered non-USA) but reject explicit foreign
            if record.country_of_incorporation:
                return True

        if any([
            record.is_educational_institution,
            record.is_federal_agency,
            record.is_airport_authority,
            record.is_council_of_governments,
            record.is_community_dev_corp,
            record.is_federally_funded_rd,
        ]):
            return True

        # Name-regex check against contractor_name (primary display name)
        for pat in _NON_PUBLIC_RE:
            if pat.search(record.contractor_name) or pat.search(record.legal_business_name):
                return True

        return False

    # ── Tier 1: CAGE → GLEIF → LEI → OpenFIGI ────────────────────────────────

    def _resolve_via_cage(self, record: ContractRecord) -> dict:
        primary_name = record.contractor_name or record.legal_business_name
        cage_result = self.cage_resolver.resolve_cage(record.cage_code)
        if not cage_result.get("lei"):
            return {}

        lei = cage_result["lei"]
        lei_result = self.lei_resolver.resolve_lei(lei)
        if not lei_result.get("ticker"):
            return {}

        ticker = lei_result["ticker"]
        cik = lei_result.get("cik", "")
        mc = self._get_market_cap(ticker)
        norm = _normalize(primary_name)
        audit = [
            {"path": "cage_to_lei", "source": "GLEIF", "lei": lei,
             "confidence": cage_result.get("confidence")},
            {"path": "lei_to_ticker", "source": "OpenFIGI", "ticker": ticker,
             "confidence": lei_result.get("confidence")},
        ]
        return self._make_result(primary_name, norm, ticker, cik, "high",
                                  "cage_lei_openfigi", None, mc, audit)

    # ── Tier 2: multi-name EDGAR exact match ─────────────────────────────────

    def _exact_match(self, name: str) -> dict | None:
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)

        candidate = None
        for key in [name.strip().upper(), norm, stripped]:
            if key in self.edgar_map:
                candidate = self.edgar_map[key]
                break
        if not candidate and stripped in self._stripped_map:
            _, candidate = self._stripped_map[stripped]

        if not candidate:
            return None

        cik = candidate.get("cik", "")
        ticker = candidate["ticker"]
        if cik:
            valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
            if valid:
                mc = self._get_market_cap(ticker)
                return self._make_result(name, norm, ticker, cik, confidence, evidence, None, mc)
        else:
            mc = self._get_market_cap(ticker)
            if mc > 0:
                return self._make_result(name, norm, ticker, "", "medium",
                                          "exact_edgar_map_unverified", None, mc)
        return None

    # ── Tier 3: multi-name fuzzy match ───────────────────────────────────────

    def _fuzzy_match(self, name: str) -> dict | None:
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)
        min_score = 80 if len(stripped.split()) <= 3 else 85

        results = process.extract(norm, self._edgar_names,
                                   scorer=fuzz.token_sort_ratio, limit=5)
        for match_name, score, _ in results:
            if score < min_score:
                break
            entry = self.edgar_map[match_name]
            cik = entry.get("cik", "")
            ticker = entry["ticker"]

            if score >= 95:
                mc = self._get_market_cap(ticker)
                if mc > 0:
                    return self._make_result(name, norm, ticker, cik,
                                              "medium_high", "fuzzy_very_high", None, mc)

            if cik:
                valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
                if valid:
                    mc = self._get_market_cap(ticker)
                    return self._make_result(name, norm, ticker, cik,
                                              confidence, f"fuzzy_{evidence}", None, mc)
        return None

    # ── Tier 4: substring match ───────────────────────────────────────────────

    def _substring_match(self, name: str) -> dict | None:
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)

        best_match = None
        best_len = 0
        for edgar_stripped, edgar_orig, entry in self._substr_candidates:
            match_len = 0
            if edgar_stripped in stripped:
                match_len = len(edgar_stripped)
            elif stripped in edgar_stripped:
                match_len = len(stripped)
            if match_len > best_len:
                best_match = (edgar_stripped, edgar_orig, entry)
                best_len = match_len

        if not best_match or best_len < 10:
            return None

        edgar_stripped, _, entry = best_match
        longer = max(len(stripped), len(edgar_stripped))
        if best_len / longer < 0.6:
            return None

        ticker = entry["ticker"]
        cik = entry.get("cik", "")
        mc = self._get_market_cap(ticker)
        if mc > 0:
            return self._make_result(name, norm, ticker, cik, "medium",
                                      "substring_match", None, mc)
        return None

    # ── Market cap ────────────────────────────────────────────────────────────

    def _get_market_cap(self, ticker: str) -> float:
        if ticker in self.mcap_cache:
            return float(self.mcap_cache[ticker])
        try:
            mcap = float(yf.Ticker(ticker).fast_info.market_cap or 0)
            self.mcap_cache[ticker] = mcap
            self._mcap_unsaved += 1
            if self._mcap_unsaved >= _MCAP_CACHE_WRITE_BATCH:
                self._flush_mcap_cache()
            return mcap
        except Exception as e:
            log.debug(f"Market cap fetch failed for {ticker}: {e}")
            return 0.0

    # ── Result builder ────────────────────────────────────────────────────────

    @staticmethod
    def _make_result(original, normalized, ticker, cik, confidence,
                     evidence_type, rejection_reason=None, market_cap=0.0,
                     audit_trail=None) -> dict:
        return {
            "original_name":      original,
            "normalized_name":    normalized,
            "resolved_ticker":    ticker,
            "resolved_cik":       cik or "",
            "evidence_type":      evidence_type,
            "confidence":         confidence,
            "rejection_reason":   rejection_reason,
            "market_cap_current": market_cap or 0.0,
            "audit_trail":        audit_trail or [],
            "last_verified":      datetime.utcnow().isoformat(),
        }
```

- [ ] **Step 4: Run Tier 0 tests — all should pass**

```bash
rtk python -m pytest tests/test_ticker_resolver_v4.py -v -k "tier0"
```

Expected: 10 tests PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add ticker_resolver_v4.py tests/test_ticker_resolver_v4.py
rtk git commit -m "feat: Add TickerResolverV4 scaffold with Tier 0 non-public detection"
```

---

## Task 4: Add and Test Tier 1 (CAGE Path)

**Files:**
- Modify: `tests/test_ticker_resolver_v4.py` (add tests)
- Tier 1 code is already written in Task 3 — just add tests to verify it

- [ ] **Step 1: Add Tier 1 tests to `tests/test_ticker_resolver_v4.py`**

Append these tests to the file:

```python
# ─── Tier 1 tests ─────────────────────────────────────────────────────────────

def test_tier1_cage_resolves_to_ticker():
    """CAGE → GLEIF → LEI → OpenFIGI happy path."""
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {
        "lei": "ABCDE12345FGHIJ67890",
        "confidence": 0.95,
        "rejection_reason": None,
        "source": "gleif",
    }
    lei_mock = MagicMock()
    lei_mock.resolve_lei.return_value = {
        "ticker": "ACME",
        "cik": "0001234567",
        "confidence": "high",
    }
    r = make_resolver()
    r.cage_resolver = cage_mock
    r.lei_resolver = lei_mock
    r.mcap_cache["ACME"] = 500_000_000.0  # inject cached mcap

    record = make_record(cage_code="1RBX4")
    result = r.resolve(record)

    assert result["resolved_ticker"] == "ACME"
    assert result["confidence"] == "high"
    assert result["evidence_type"] == "cage_lei_openfigi"
    cage_mock.resolve_cage.assert_called_once_with("1RBX4")
    lei_mock.resolve_lei.assert_called_once_with("ABCDE12345FGHIJ67890")


def test_tier1_cage_gleif_miss_falls_through():
    """If GLEIF returns no LEI, fall through to Tier 2."""
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {
        "lei": None,
        "confidence": 0,
        "rejection_reason": "not_found_in_gleif",
        "source": "none",
    }
    r = make_resolver()
    r.cage_resolver = cage_mock
    record = make_record(cage_code="1RBX4", contractor_name="XYZZY NO MATCH CO")
    result = r.resolve(record)

    # Should not crash — falls to unresolved
    assert result["resolved_ticker"] is None
    assert result["evidence_type"] == "unresolved"


def test_tier1_empty_cage_skips_to_tier2():
    """Empty CAGE code skips Tier 1 entirely."""
    cage_mock = MagicMock()
    r = make_resolver()
    r.cage_resolver = cage_mock
    record = make_record(cage_code="")
    r.resolve(record)

    cage_mock.resolve_cage.assert_not_called()
```

- [ ] **Step 2: Run Tier 1 tests**

```bash
rtk python -m pytest tests/test_ticker_resolver_v4.py -v -k "tier1"
```

Expected: 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
rtk git add tests/test_ticker_resolver_v4.py
rtk git commit -m "test: Add Tier 1 CAGE resolution tests for TickerResolverV4"
```

---

## Task 5: Add and Test Tier 2 (Multi-name EDGAR Exact Match)

**Files:**
- Modify: `tests/test_ticker_resolver_v4.py` (add tests)

Tier 2 code is already in `ticker_resolver_v4.py` from Task 3. These tests verify the multi-name priority order and SEC validation.

- [ ] **Step 1: Add Tier 2 tests to `tests/test_ticker_resolver_v4.py`**

Append these tests:

```python
# ─── Tier 2 tests ─────────────────────────────────────────────────────────────

def test_tier2_exact_legal_name_match():
    """Legal business name exact-matches EDGAR map."""
    edgar_map = {"LOCKHEED MARTIN CORPORATION": {"ticker": "LMT", "cik": "0000936468"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["LMT"] = 100_000_000_000.0

    record = make_record(
        cage_code="",  # skip Tier 1
        legal_business_name="LOCKHEED MARTIN CORPORATION",
        contractor_name="LOCKHEED MARTIN",
    )
    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(record)

    assert result["resolved_ticker"] == "LMT"
    assert result["confidence"] == "high"


def test_tier2_contractor_name_fallback_when_legal_misses():
    """Contractor name resolves when legal_business_name has no EDGAR match."""
    edgar_map = {"NORTHROP GRUMMAN CORPORATION": {"ticker": "NOC", "cik": "0001133421"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["NOC"] = 50_000_000_000.0

    record = make_record(
        cage_code="",
        legal_business_name="NORTHROP GRUMMAN SYSTEMS CORP",  # no exact match
        contractor_name="NORTHROP GRUMMAN CORPORATION",       # exact match
    )
    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(record)

    assert result["resolved_ticker"] == "NOC"


def test_tier2_dba_name_used_when_other_names_miss():
    """DBA name resolves when legal and contractor names have no match."""
    edgar_map = {"GENERAL DYNAMICS CORPORATION": {"ticker": "GD", "cik": "0000040533"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["GD"] = 60_000_000_000.0

    record = make_record(
        cage_code="",
        legal_business_name="GD ADVANCED INFORMATION SYSTEMS LLC",
        contractor_name="GD AIS LLC",
        dba_name="GENERAL DYNAMICS CORPORATION",
    )
    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(record)

    assert result["resolved_ticker"] == "GD"


def test_tier2_parent_name_used_as_last_resort():
    """Parent name resolves when all direct names miss."""
    edgar_map = {"RAYTHEON TECHNOLOGIES CORPORATION": {"ticker": "RTX", "cik": "0000101829"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["RTX"] = 130_000_000_000.0

    record = make_record(
        cage_code="",
        legal_business_name="RAYTHEON INTELLIGENCE AND SPACE LLC",
        contractor_name="RTX MISSION SYSTEMS",
        dba_name="",
        parent_name="RAYTHEON TECHNOLOGIES CORPORATION",
    )
    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(record)

    assert result["resolved_ticker"] == "RTX"
```

- [ ] **Step 2: Run Tier 2 tests**

```bash
rtk python -m pytest tests/test_ticker_resolver_v4.py -v -k "tier2"
```

Expected: 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
rtk git add tests/test_ticker_resolver_v4.py
rtk git commit -m "test: Add Tier 2 multi-name EDGAR exact match tests for V4"
```

---

## Task 6: Add and Test Tiers 3–5 + Cache Key

**Files:**
- Modify: `tests/test_ticker_resolver_v4.py` (add tests)

- [ ] **Step 1: Append Tier 3–5 and cache key tests**

```python
# ─── Tier 3 tests ─────────────────────────────────────────────────────────────

def test_tier3_fuzzy_match_resolves():
    """Fuzzy match finds ticker when exact match fails."""
    edgar_map = {"BOEING COMPANY": {"ticker": "BA", "cik": "0000012927"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["BA"] = 90_000_000_000.0

    # "BOEING CO" won't exact-match "BOEING COMPANY" but should fuzzy-match
    record = make_record(
        cage_code="",
        legal_business_name="BOEING CO",
        contractor_name="BOEING CO",
    )
    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(record)

    assert result["resolved_ticker"] == "BA"
    assert "fuzzy" in result["evidence_type"]


# ─── Tier 4 tests ─────────────────────────────────────────────────────────────

def test_tier4_subsidiary_substring_match():
    """Subsidiary name resolves via parent ticker through substring matching."""
    edgar_map = {"NORTHROP GRUMMAN": {"ticker": "NOC", "cik": "0001133421"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["NOC"] = 50_000_000_000.0

    record = make_record(
        cage_code="",
        legal_business_name="NORTHROP GRUMMAN SYSTEMS CORPORATION",
        contractor_name="NORTHROP GRUMMAN SYSTEMS CORPORATION",
    )
    result = r.resolve(record)

    assert result["resolved_ticker"] == "NOC"
    assert result["evidence_type"] == "substring_match"


# ─── Tier 5 tests ─────────────────────────────────────────────────────────────

def test_tier5_sole_source_tagged_when_num_offers_is_1():
    r = make_resolver()
    record = make_record(cage_code="", num_offers="1")
    result = r.resolve(record)
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "sole_source_unresolved"


def test_tier5_not_competed_code_tagged():
    r = make_resolver()
    record = make_record(cage_code="", extent_competed_code="B", num_offers="1")
    result = r.resolve(record)
    assert result["rejection_reason"] == "sole_source_unresolved"


def test_tier5_normal_unresolved_tagged_no_match():
    r = make_resolver()
    record = make_record(cage_code="", num_offers="5")
    result = r.resolve(record)
    assert result["rejection_reason"] == "no_match"


# ─── Cache key tests ──────────────────────────────────────────────────────────

def test_cache_key_prefers_cage_over_name():
    """Same CAGE code → cache hit even with different contractor name."""
    r = make_resolver()
    record1 = make_record(cage_code="1RBX4", contractor_name="ACME INC")
    record2 = make_record(cage_code="1RBX4", contractor_name="ACME DEFENSE INC")

    result1 = r.resolve(record1)
    # Second call should hit cache (no second computation needed)
    result2 = r.resolve(record2)

    assert result1 is result2  # same object from cache


def test_cache_key_uses_uei_when_no_cage():
    """UEI used as cache key when CAGE is empty."""
    r = make_resolver()
    record1 = make_record(cage_code="", uei="ABCDEF123456", contractor_name="ACME INC")
    record2 = make_record(cage_code="", uei="ABCDEF123456", contractor_name="ACME DEFENSE INC")

    result1 = r.resolve(record1)
    result2 = r.resolve(record2)

    assert result1 is result2
```

- [ ] **Step 2: Run all V4 tests**

```bash
rtk python -m pytest tests/test_ticker_resolver_v4.py -v
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
rtk git add tests/test_ticker_resolver_v4.py
rtk git commit -m "test: Add Tier 3-5 and cache key tests for TickerResolverV4"
```

---

## Task 7: Update build_training_set.py Stage 1

**Files:**
- Modify: `build_training_set.py` (lines ~160–337)

Stage 1 replaces the ZIP/CSV reader with `read_sam_gov_csv()` and returns both awards and a records dict for Stage 2.

- [ ] **Step 1: Add imports and constants at top of `build_training_set.py`**

In `build_training_set.py`, find the imports block (around line 30). Add after the existing imports:

```python
from sam_gov_reader import ContractRecord, read_sam_gov_csv, find_sam_gov_csv
```

Remove the `import zipfile` line (no longer needed).

- [ ] **Step 2: Add `_record_to_award_dict` helper function**

Add this function before `stage1_load_and_filter` (around line 155):

```python
# Extent Competed codes that indicate no competition
_NOT_COMPETED_CODES = {"B", "C", "G", "CDO", "URG", "SP2"}


def _record_to_award_dict(record: ContractRecord) -> dict:
    """Map a ContractRecord to the award dict schema expected by Stages 2 and 3."""
    sole_source = (
        record.extent_competed_code.upper() in _NOT_COMPETED_CODES
        or bool(record.other_than_full_open.strip())
    )
    return {
        "award_key":                record.piid,
        "award_id":                 record.piid,
        "posted_date":              record.posted_date,
        "awardee_name":             record.contractor_name or record.legal_business_name,
        "award_amount":             record.award_amount,
        "agency":                   record.agency,
        "sub_agency":               "",
        "naics":                    record.naics_code,
        "naics_description":        record.naics_description,
        "set_aside":                record.set_aside_code,
        "extent_competed":          record.extent_competed_code,
        "sole_source":              sole_source,
        "is_idiq":                  False,  # reader filters these out
        "parent_recipient_name":    record.parent_name,
        # New columns (V4 additions)
        "cage_code":                record.cage_code,
        "uei":                      record.uei,
        "legal_business_name":      record.legal_business_name,
        "dba_name":                 record.dba_name,
        "country_of_incorporation": record.country_of_incorporation,
    }
```

- [ ] **Step 3: Replace `stage1_load_and_filter` body**

Replace the entire `stage1_load_and_filter` function (lines ~252–337) with:

```python
def stage1_load_and_filter() -> tuple[list[dict], dict[str, ContractRecord]]:
    """Read SAM.gov CSV, filter by amount, remove top-N and IDV, write filtered CSV.

    Returns:
        (awards, records_by_key) where awards is the list of award dicts and
        records_by_key maps award_key (PIID) → ContractRecord for Stage 2.
    """
    log.info("=" * 60)
    log.info("STAGE 1: LOAD & FILTER")
    log.info(f"  Range  : ${MIN_CONTRACT_VALUE/1e6:.0f}M – ${MAX_AWARD_AMOUNT/1e9:.0f}B")
    log.info(f"  Remove : top {TOP_N_TO_REMOVE} companies by contract count")
    log.info(f"  Remove : IDV umbrellas (filtered by reader)")
    log.info(f"  Output : {os.path.basename(FILTERED_CSV)}")
    log.info("=" * 60)
    t0 = time.time()

    sam_csv = find_sam_gov_csv(DATASET_DIR)
    log.info(f"  Source: {os.path.basename(sam_csv)}")

    awards_by_key: dict[str, dict] = {}
    records_by_key: dict[str, ContractRecord] = {}
    total_rows = 0

    for record in read_sam_gov_csv(sam_csv):
        total_rows += 1
        key = record.piid
        if not key:
            continue
        awards_by_key[key] = _record_to_award_dict(record)
        records_by_key[key] = record
        if total_rows % 100_000 == 0:
            log.info(f"  ... {total_rows:,} rows read, {len(awards_by_key):,} unique awards so far")

    awards = list(awards_by_key.values())
    after_dedup = len(awards)
    log.info(f"  {total_rows:,} rows read → {after_dedup:,} unique awards (dedup + filters)")

    # Remove top-N companies by contract count
    name_counts = Counter(a["awardee_name"] for a in awards)
    top_names = {name for name, _ in name_counts.most_common(TOP_N_TO_REMOVE)}
    log.info(f"  Top {TOP_N_TO_REMOVE} companies removed (by contract volume):")
    for name, cnt in name_counts.most_common(TOP_N_TO_REMOVE):
        log.info(f"    {cnt:>6,}  {name}")

    awards = [a for a in awards if a["awardee_name"] not in top_names]
    # Also remove corresponding records from records_by_key
    top_keys = {a["award_key"] for a in awards_by_key.values()
                if a["awardee_name"] in top_names}
    for k in top_keys:
        records_by_key.pop(k, None)

    dropped_top20 = after_dedup - len(awards)
    log.info(f"  Dropped {dropped_top20:,} contracts from top-{TOP_N_TO_REMOVE} companies")
    log.info(f"  Final: {len(awards):,} contracts")

    _write_csv(FILTERED_CSV, awards)
    _save_cp(CP_STAGE1, {
        "total_rows_read": total_rows,
        "unique_after_dedup_and_filter": after_dedup,
        "dropped_top20": dropped_top20,
        "final_count": len(awards),
    })

    log.info(f"Stage 1 complete in {_elapsed(t0)}")
    return awards, records_by_key
```

- [ ] **Step 4: Update `main()` to use new Stage 1 signature**

Find in `main()`:
```python
year = 2023
month_filter = 0  # All months (full year)

# Stage 1 — load & filter (fast, reads local file)
awards = stage1_load_and_filter(year, month_filter=month_filter)
```

Replace with:
```python
# Stage 1 — load & filter (fast, reads local SAM.gov CSV)
awards, records_by_key = stage1_load_and_filter()
```

Also update the Stage 2 call below it:
```python
# OLD
awards = stage2_resolve_tickers(awards)

# NEW
awards = stage2_resolve_tickers(awards, records_by_key)
```

- [ ] **Step 5: Verify Stage 1 imports cleanly**

```bash
rtk python -c "from build_training_set import stage1_load_and_filter, _record_to_award_dict; print('OK')"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
rtk git add build_training_set.py
rtk git commit -m "feat: Update Stage 1 to read SAM.gov CSV via sam_gov_reader"
```

---

## Task 8: Update build_training_set.py Stage 2

**Files:**
- Modify: `build_training_set.py` (Stage 2 function, lines ~411–505)

- [ ] **Step 1: Replace `stage2_resolve_tickers` signature and resolver**

Find:
```python
def stage2_resolve_tickers(awards: list[dict]) -> list[dict]:
```

Replace with:
```python
def stage2_resolve_tickers(awards: list[dict],
                           records_by_key: dict[str, "ContractRecord"]) -> list[dict]:
```

Find:
```python
    from ticker_resolver_v3 import TickerResolverV3
```

Replace with:
```python
    from ticker_resolver_v4 import TickerResolverV4
```

Find:
```python
    edgar_map = _load_edgar_map()
    resolver  = TickerResolverV3(edgar_map, cache_path=TICKER_CACHE_V2_FILE)
```

Replace with:
```python
    TICKER_CACHE_V4_FILE = os.path.join(ROOT, ".ticker_cache_v4.json")
    edgar_map = _load_edgar_map()
    resolver  = TickerResolverV4(edgar_map, cache_path=TICKER_CACHE_V4_FILE)
```

- [ ] **Step 2: Replace deduplication key and resolve call**

Find the deduplication block (around line 436–464):
```python
    # Deduplicate: resolve once per unique company name, then map to all awards
    # Build name → [award_keys] mapping for awards not yet in checkpoint
    name_to_keys: dict[str, list[str]] = {}
    skipped_count = 0
    for award in awards:
        key = award["award_key"]
        if key in cp:
            skipped_count += 1
        else:
            name = award["awardee_name"]
            name_to_keys.setdefault(name, []).append(key)

    unique_names = list(name_to_keys.keys())
    log.info(f"  {len(awards):,} awards → {len(unique_names):,} unique names to resolve "
             f"({skipped_count:,} from checkpoint)")
```

Replace with:
```python
    # Deduplicate by V4 cache key (cage_code > uei > legal_name > contractor_name)
    # This avoids re-resolving the same entity under different award keys
    def _v4_cache_key(award: dict) -> str:
        record = records_by_key.get(award["award_key"])
        if record:
            return (record.cage_code or record.uei or
                    record.legal_business_name or record.contractor_name)
        return award["awardee_name"]

    cachekey_to_keys: dict[str, list[str]] = {}
    skipped_count = 0
    for award in awards:
        key = award["award_key"]
        if key in cp:
            skipped_count += 1
        else:
            ck = _v4_cache_key(award)
            cachekey_to_keys.setdefault(ck, []).append(key)

    unique_keys = list(cachekey_to_keys.keys())
    log.info(f"  {len(awards):,} awards → {len(unique_keys):,} unique entities to resolve "
             f"({skipped_count:,} from checkpoint)")
```

- [ ] **Step 3: Replace resolution loop**

Find the resolution loop (around line 451–488):
```python
    # Build name → parent_name mapping for parent escalation
    name_to_parent: dict[str, str] = {}
    for award in awards:
        name = award["awardee_name"]
        parent = award.get("parent_recipient_name", "")
        if parent and name not in name_to_parent:
            name_to_parent[name] = parent

    for i, name in enumerate(unique_names):
        parent = name_to_parent.get(name, "")
        result = resolver.resolve(name, parent_name=parent)
        ticker = result.get("resolved_ticker") or ""
        entry  = {
            "ticker":            ticker,
            "cik":               result.get("resolved_cik") or "",
            "ticker_confidence": result.get("confidence", "none"),
        }

        # Apply to all awards with this name
        for key in name_to_keys[name]:
            cp[key] = entry
```

Replace with:
```python
    # Build cache_key → representative ContractRecord mapping
    ck_to_record: dict[str, "ContractRecord"] = {}
    for award in awards:
        key = award["award_key"]
        if key not in records_by_key:
            continue
        ck = _v4_cache_key(award)
        if ck not in ck_to_record:
            ck_to_record[ck] = records_by_key[key]

    for i, ck in enumerate(unique_keys):
        record = ck_to_record.get(ck)
        if record:
            result = resolver.resolve(record)
        else:
            result = {"resolved_ticker": None, "confidence": "none",
                      "resolved_cik": "", "evidence_type": "unresolved"}

        ticker = result.get("resolved_ticker") or ""
        entry  = {
            "ticker":            ticker,
            "cik":               result.get("resolved_cik") or "",
            "ticker_confidence": result.get("confidence", "none"),
        }

        # Apply to all awards with this cache key
        for key in cachekey_to_keys[ck]:
            cp[key] = entry
```

- [ ] **Step 4: Fix progress log line** (references `unique_names` — update to `unique_keys`)

Find:
```python
            pct = (i + 1) / len(unique_names) * 100
            pct_resolved = resolved_count / (i + 1) * 100 if (i + 1) > 0 else 0
            log.info(f"  [{i+1:,}/{len(unique_names):,} — {pct:.1f}%] "
```

Replace with:
```python
            pct = (i + 1) / len(unique_keys) * 100
            pct_resolved = resolved_count / (i + 1) * 100 if (i + 1) > 0 else 0
            log.info(f"  [{i+1:,}/{len(unique_keys):,} — {pct:.1f}%] "
```

Find:
```python
    pct_resolved = resolved_count / len(unique_names) * 100 if unique_names else 0
```

Replace with:
```python
    pct_resolved = resolved_count / len(unique_keys) * 100 if unique_keys else 0
```

- [ ] **Step 5: Verify Stage 2 imports cleanly**

```bash
rtk python -c "from build_training_set import stage2_resolve_tickers; print('OK')"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
rtk git add build_training_set.py
rtk git commit -m "feat: Update Stage 2 to use TickerResolverV4 with ContractRecord"
```

---

## Task 9: Integration Smoke Test

Verify the full Stage 1 pipeline runs against the real SAM.gov CSV and produces correct output.

- [ ] **Step 1: Run Stage 1 only and inspect output**

```bash
rtk python -c "
import logging
logging.basicConfig(level=logging.INFO)
from build_training_set import stage1_load_and_filter
awards, records_by_key = stage1_load_and_filter()
print(f'Awards: {len(awards)}')
print(f'Records: {len(records_by_key)}')
# Verify CAGE codes are present
cage_count = sum(1 for a in awards if a.get('cage_code'))
print(f'Awards with CAGE code: {cage_count} ({cage_count/len(awards)*100:.1f}%)')
# Verify no foreign entities
countries = set(a.get('country_of_incorporation') for a in awards)
print(f'Countries in output: {countries}')
# Verify new columns present
sample = awards[0]
for col in ['cage_code', 'uei', 'legal_business_name', 'dba_name']:
    print(f'  {col}: {repr(sample.get(col))}')
"
```

Expected:
- Awards count > 0
- CAGE code fill rate > 50%
- Countries: `{'USA'}` only
- New columns present in sample row

- [ ] **Step 2: Verify filtered_training_set.csv has new columns**

```bash
rtk python -c "
import csv
with open('datasets/filtered_training_set.csv') as f:
    r = csv.DictReader(f)
    print('Has cage_code:', 'cage_code' in r.fieldnames)
    print('Has uei:', 'uei' in r.fieldnames)
    print('Has legal_business_name:', 'legal_business_name' in r.fieldnames)
    row = next(r)
    print('Sample CAGE:', row.get('cage_code'))
    print('Sample country:', row.get('country_of_incorporation'))
"
```

Expected: all `True`, sample CAGE code non-empty, country is `"USA"`.

- [ ] **Step 3: Run full test suite to confirm nothing broken**

```bash
rtk python -m pytest tests/ -v --tb=short
```

Expected: all existing tests + new tests PASS. If `test_integration_multi_identifier.py` makes live API calls and fails due to network, that's acceptable — it was pre-existing.

- [ ] **Step 4: Final commit**

```bash
rtk git add -A
rtk git commit -m "feat: Complete SAM.gov CSV + TickerResolverV4 integration"
```

---

## Summary

| Task | Files | Key outcome |
|---|---|---|
| 1 | `datasets/FirstReport.csv` | CSV placed and verified |
| 2 | `sam_gov_reader.py`, `tests/test_sam_gov_reader.py` | Reader + 9 tests passing |
| 3 | `ticker_resolver_v4.py`, `tests/test_ticker_resolver_v4.py` | V4 scaffold + Tier 0 (10 tests) |
| 4 | `tests/test_ticker_resolver_v4.py` | Tier 1 CAGE tests (3 tests) |
| 5 | `tests/test_ticker_resolver_v4.py` | Tier 2 multi-name tests (4 tests) |
| 6 | `tests/test_ticker_resolver_v4.py` | Tier 3–5 + cache tests (7 tests) |
| 7 | `build_training_set.py` | Stage 1 swapped to SAM.gov reader |
| 8 | `build_training_set.py` | Stage 2 swapped to V4 resolver |
| 9 | — | Smoke test confirms end-to-end works |
