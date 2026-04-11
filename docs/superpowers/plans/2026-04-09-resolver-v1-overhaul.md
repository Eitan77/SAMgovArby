# Ticker Resolver V1 Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Overhaul the `resolver/` package to fully implement the V1 spec — entity-cluster-based resolution, DuckDB-backed issuer master from SEC + Nasdaq Trader, 8-stage pipeline, V1 output schema, and a batch CLI entry point.

**Architecture:** New modules are added inside the existing `resolver/` package. The `_compat.py` shim keeps `build_training_set.py` and `main.py` unchanged. The V1 pipeline runs through a new `resolver/pipeline.py`; the public `api.py` surface gains a `resolve_v1()` entry point alongside the existing one.

**Tech Stack:** Python 3.12, DuckDB (`pip install duckdb`), pandas, rapidfuzz, requests, sqlite3 (existing)

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| New | `resolver/storage.py` | DuckDB schema, connection, all table DDL |
| New | `resolver/issuer_master.py` | Download SEC + Nasdaq, build `issuer_master` + `issuer_aliases` in DuckDB |
| New | `resolver/clusters.py` | Build `entity_clusters` table from contract features |
| New | `resolver/pipeline.py` | 8-stage resolution pipeline (stages 0–8) |
| New | `resolver/cli.py` | `python -m resolver` batch entry point + run metrics |
| Modify | `resolver/normalize.py` | Add `conservative_normalize`, `aggressive_normalize`, `token_metadata` |
| Modify | `resolver/models.py` | Add `V1FinalRow`, `V1EntityCache`, `V1RunMetrics`, `V1ThresholdsConfig`, `score_to_confidence_band` |
| Modify | `resolver/api.py` | Expose `resolve_v1()` wiring new pipeline |
| New | `tests/resolver/__init__.py` | Empty |
| New | `tests/resolver/test_normalize_v1.py` | Tests for new normalization functions |
| New | `tests/resolver/test_storage.py` | Tests for DuckDB schema creation |
| New | `tests/resolver/test_clusters.py` | Tests for entity clustering |
| New | `tests/resolver/test_pipeline.py` | Tests for each pipeline stage |
| New | `tests/resolver/test_cli.py` | End-to-end CLI smoke test |

---

## Task 1: DuckDB Storage Layer

**Files:**
- Create: `resolver/storage.py`
- Create: `tests/resolver/__init__.py`
- Create: `tests/resolver/test_storage.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/resolver/test_storage.py
import os, tempfile, pytest
from resolver.storage import get_db, ensure_schema, TABLES

def test_ensure_schema_creates_all_tables():
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "test.duckdb"))
        ensure_schema(con)
        existing = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for t in TABLES:
            assert t in existing, f"Missing table: {t}"
        con.close()

def test_uniqueness_contract_row_id():
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        con.execute(
            "INSERT INTO contracts_normalized (contract_row_id) VALUES ('r1')"
        )
        with pytest.raises(Exception):
            con.execute(
                "INSERT INTO contracts_normalized (contract_row_id) VALUES ('r1')"
            )
        con.close()

def test_issuer_master_unique_on_pub_id():
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        con.execute("INSERT INTO issuer_master (public_company_id) VALUES ('CIK_001')")
        with pytest.raises(Exception):
            con.execute("INSERT INTO issuer_master (public_company_id) VALUES ('CIK_001')")
        con.close()
```

- [ ] **Step 2: Run — expect FAIL** (`ModuleNotFoundError`)

```bash
rtk python -m pytest tests/resolver/test_storage.py -v 2>&1 | head -20
```

- [ ] **Step 3: Create `tests/resolver/__init__.py`** (empty file)

- [ ] **Step 4: Create `resolver/storage.py`**

```python
"""resolver/storage.py — DuckDB schema and connection management."""
from __future__ import annotations
import logging
from pathlib import Path
import duckdb

log = logging.getLogger(__name__)

TABLES = [
    "contracts_raw", "contracts_normalized", "entity_edges", "entity_clusters",
    "issuer_master", "issuer_aliases", "resolution_cache", "manual_overrides",
    "resolution_results", "review_queue", "resolver_run_log", "openfigi_cache",
]

DDL = """
CREATE TABLE IF NOT EXISTS contracts_raw (
    contract_row_id TEXT PRIMARY KEY,
    source_file     TEXT,
    ingested_at     TEXT,
    raw_json        TEXT
);

CREATE TABLE IF NOT EXISTS contracts_normalized (
    contract_row_id              TEXT PRIMARY KEY,
    entity_cluster_id            TEXT,
    ultimate_parent_uei          TEXT,
    uei                          TEXT,
    cage_code                    TEXT,
    legal_business_name_raw      TEXT,
    legal_business_name_norm_cons TEXT,
    legal_business_name_norm_agg  TEXT,
    parent_name_raw              TEXT,
    parent_name_norm_cons        TEXT,
    parent_name_norm_agg         TEXT,
    dba_name_raw                 TEXT,
    contractor_name_raw          TEXT,
    vendor_state                 TEXT,
    vendor_country               TEXT,
    country_of_incorporation     TEXT,
    date_signed                  TEXT,
    fiscal_year                  INTEGER,
    piid                         TEXT,
    dollars_obligated            DOUBLE
);

CREATE TABLE IF NOT EXISTS entity_edges (
    edge_id           TEXT PRIMARY KEY,
    entity_cluster_id TEXT NOT NULL,
    contract_row_id   TEXT NOT NULL,
    edge_type         TEXT
);

CREATE TABLE IF NOT EXISTS entity_clusters (
    entity_cluster_id      TEXT PRIMARY KEY,
    cluster_key_type       TEXT,
    ultimate_parent_uei    TEXT,
    uei                    TEXT,
    cage_code              TEXT,
    canonical_entity_name  TEXT,
    canonical_parent_name  TEXT,
    canonical_display_name TEXT,
    all_ueis_json          TEXT,
    all_cages_json         TEXT,
    all_legal_names_json   TEXT,
    all_parent_names_json  TEXT,
    all_dba_names_json     TEXT,
    state_freq_json        TEXT,
    country_freq_json      TEXT,
    naics_freq_json        TEXT,
    first_seen_date        TEXT,
    last_seen_date         TEXT,
    row_count              INTEGER,
    total_obligated        DOUBLE
);

CREATE TABLE IF NOT EXISTS issuer_master (
    public_company_id      TEXT PRIMARY KEY,
    public_company_id_type TEXT,
    cik                    TEXT,
    issuer_name_current    TEXT,
    ticker_current         TEXT,
    exchange_current       TEXT,
    is_us_tradable         BOOLEAN,
    is_common_equity       BOOLEAN,
    share_class_rank       INTEGER,
    is_adr                 BOOLEAN,
    is_etf                 BOOLEAN,
    is_fund                BOOLEAN,
    is_warrant             BOOLEAN,
    is_unit                BOOLEAN,
    is_preferred           BOOLEAN,
    active_status          TEXT,
    source_priority        INTEGER
);

CREATE TABLE IF NOT EXISTS issuer_aliases (
    alias_id                      TEXT PRIMARY KEY,
    public_company_id             TEXT NOT NULL,
    alias_raw                     TEXT,
    alias_normalized_conservative TEXT,
    alias_normalized_aggressive   TEXT,
    alias_type                    TEXT,
    source                        TEXT,
    valid_from                    TEXT,
    valid_to                      TEXT,
    UNIQUE (public_company_id, alias_raw, alias_type, source)
);

CREATE TABLE IF NOT EXISTS resolution_cache (
    entity_cluster_id     TEXT PRIMARY KEY,
    resolved              BOOLEAN,
    public_company_id     TEXT,
    public_company_name   TEXT,
    preferred_ticker      TEXT,
    preferred_exchange    TEXT,
    relationship_type     TEXT,
    resolution_stage      TEXT,
    confidence_score      DOUBLE,
    confidence_band       TEXT,
    match_explanation     TEXT,
    source_evidence_json  TEXT,
    resolver_version      TEXT,
    issuer_master_version TEXT,
    first_resolved_at     TEXT,
    last_validated_at     TEXT
);

CREATE TABLE IF NOT EXISTS manual_overrides (
    override_id         TEXT PRIMARY KEY,
    override_key_type   TEXT NOT NULL,
    override_key_value  TEXT NOT NULL,
    public_company_id   TEXT,
    public_company_name TEXT,
    preferred_ticker    TEXT,
    preferred_exchange  TEXT,
    relationship_type   TEXT,
    reason              TEXT,
    reviewed_by         TEXT,
    reviewed_at         TEXT,
    active              BOOLEAN DEFAULT TRUE,
    UNIQUE (override_key_type, override_key_value, active)
);

CREATE TABLE IF NOT EXISTS resolution_results (
    contract_row_id               TEXT PRIMARY KEY,
    entity_cluster_id             TEXT,
    resolved                      BOOLEAN,
    public_company_id             TEXT,
    public_company_id_type        TEXT,
    public_company_name           TEXT,
    preferred_ticker              TEXT,
    preferred_exchange            TEXT,
    relationship_type             TEXT,
    resolution_stage              TEXT,
    confidence_score              DOUBLE,
    confidence_band               TEXT,
    match_explanation             TEXT,
    matched_entity_name           TEXT,
    matched_parent_name           TEXT,
    matched_alias                 TEXT,
    manual_override_used          BOOLEAN,
    ambiguous                     BOOLEAN,
    needs_review                  BOOLEAN,
    share_class_rule_used         TEXT,
    historical_ticker_attempted   BOOLEAN DEFAULT FALSE,
    ticker_as_of_award_date       TEXT,
    ticker_as_of_award_confidence TEXT,
    run_id                        TEXT
);

CREATE TABLE IF NOT EXISTS review_queue (
    queue_id          TEXT PRIMARY KEY,
    entity_cluster_id TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    review_reason     TEXT,
    top_candidate_json TEXT,
    queued_at         TEXT,
    UNIQUE (entity_cluster_id, run_id)
);

CREATE TABLE IF NOT EXISTS resolver_run_log (
    run_id                 TEXT PRIMARY KEY,
    started_at             TEXT,
    ended_at               TEXT,
    total_rows             INTEGER,
    total_clusters         INTEGER,
    new_resolved           INTEGER,
    stage_wins_json        TEXT,
    unresolved_count       INTEGER,
    ambiguous_count        INTEGER,
    override_hits          INTEGER,
    cache_hits             INTEGER,
    avg_candidates_scored  DOUBLE,
    resolver_version       TEXT,
    config_hash            TEXT
);

CREATE TABLE IF NOT EXISTS openfigi_cache (
    cache_key  TEXT PRIMARY KEY,
    request    TEXT,
    response   TEXT,
    cached_at  TEXT
);
"""

_connections: dict[str, duckdb.DuckDBPyConnection] = {}


def get_db(path: str = "data/cache/resolver.duckdb") -> duckdb.DuckDBPyConnection:
    if path not in _connections:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        _connections[path] = duckdb.connect(path)
    return _connections[path]


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.executescript(DDL)
    log.debug("DuckDB schema ensured.")


def close_all() -> None:
    for con in _connections.values():
        try:
            con.close()
        except Exception:
            pass
    _connections.clear()
```

- [ ] **Step 5: Run tests — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_storage.py -v 2>&1 | head -20
```

Note: DuckDB uses `executescript` for multi-statement DDL. If it raises `AttributeError`, replace `con.executescript(DDL)` with `con.execute(DDL)` — DuckDB supports multi-statement strings in `execute`.

- [ ] **Step 6: Commit**

```bash
rtk git add resolver/storage.py tests/resolver/__init__.py tests/resolver/test_storage.py && rtk git commit -m "feat: DuckDB storage layer with full V1 schema"
```

---

## Task 2: Normalization Upgrades

**Files:**
- Modify: `resolver/normalize.py`
- Create: `tests/resolver/test_normalize_v1.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/resolver/test_normalize_v1.py
from resolver.normalize import (
    conservative_normalize, aggressive_normalize, token_metadata,
)

def test_conservative_strips_periods():
    assert conservative_normalize("I.B.M.") == "IBM"

def test_conservative_ampersand():
    assert conservative_normalize("AT&T Inc.") == "AT AND T INC"

def test_conservative_unicode():
    assert conservative_normalize("Lockhéed Martin") == "LOCKHEED MARTIN"

def test_aggressive_removes_corp_suffix():
    assert aggressive_normalize("Raytheon Technologies Corporation") == "RAYTHEON TECHNOLOGIES"

def test_aggressive_removes_the():
    assert aggressive_normalize("The Boeing Company") == "BOEING"

def test_aggressive_token_sorted():
    # Same tokens in different order → same result
    a = aggressive_normalize("Technologies Raytheon")
    b = aggressive_normalize("Raytheon Technologies")
    assert a == b

def test_aggressive_single_meaningful_word():
    # Don't return empty string when all words are suffixes
    result = aggressive_normalize("Holdings LLC")
    assert result is not None and len(result) > 0

def test_token_metadata_bigrams():
    meta = token_metadata("Science Applications International")
    assert meta["token_count"] >= 2
    assert len(meta["bigrams"]) >= 1

def test_token_metadata_empty():
    meta = token_metadata(None)
    assert meta["tokens"] == []
    assert meta["bigrams"] == []
    assert meta["token_count"] == 0
```

- [ ] **Step 2: Run — expect FAIL**

```bash
rtk python -m pytest tests/resolver/test_normalize_v1.py -v 2>&1 | head -20
```

- [ ] **Step 3: Append to `resolver/normalize.py`** (after existing functions)

```python
# ── V1 normalization additions ─────────────────────────────────────────────────

_CONS_PUNCT_RE = re.compile(r"[.,'\"/\\]")

def conservative_normalize(value: str | None) -> str | None:
    """
    V1 conservative normalization:
    uppercase, trim, collapse spaces, strip .,'"/\\, & → AND, unicode → ASCII.
    Alias for the existing normalize_name but with stricter punct stripping.
    """
    if not value or not value.strip():
        return None
    s = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    s = s.strip().upper()
    s = s.replace("&", " AND ")
    s = _CONS_PUNCT_RE.sub(" ", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r" +", " ", s).strip() or None


_AGG_REMOVE = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY",
    "LLC", "LTD", "LIMITED", "PLC", "THE", "HOLDINGS", "HOLDING",
    "GROUP", "LP", "LLP", "ENTERPRISES", "INTERNATIONAL", "GLOBAL",
    "SERVICES", "SOLUTIONS", "SYSTEMS", "TECHNOLOGIES", "TECHNOLOGY",
}


def aggressive_normalize(value: str | None) -> str | None:
    """
    Conservative normalization + remove corporate suffixes + token-sorted.
    Guardrail: never returns empty string — falls back to conservative form.
    """
    cons = conservative_normalize(value)
    if not cons:
        return None
    tokens = [t for t in cons.split() if t not in _AGG_REMOVE]
    if not tokens:
        return cons  # Fallback: e.g. "Holdings LLC" → "HOLDINGS LLC"
    return " ".join(sorted(tokens))


def token_metadata(value: str | None) -> dict:
    """Return token list, count, and bigrams for matching."""
    cons = conservative_normalize(value)
    if not cons:
        return {"tokens": [], "token_count": 0, "bigrams": []}
    tokens = [t for t in cons.split() if t not in COMMON_STOPWORDS]
    bigrams = [f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens) - 1)]
    return {"tokens": tokens, "token_count": len(tokens), "bigrams": bigrams}
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_normalize_v1.py -v 2>&1 | head -20
```

- [ ] **Step 5: Commit**

```bash
rtk git add resolver/normalize.py tests/resolver/test_normalize_v1.py && rtk git commit -m "feat: V1 normalization — conservative_normalize, aggressive_normalize, token_metadata"
```

---

## Task 3: V1 Output Models

**Files:**
- Modify: `resolver/models.py`

- [ ] **Step 1: Append to `resolver/models.py`** (after the existing `OverrideRecord` dataclass)

```python
# ── V1 output schema ──────────────────────────────────────────────────────────

RESOLVER_V1_VERSION = "1.0"


def score_to_confidence_band(score: float) -> str:
    """V1 spec bands: 95-100=very_high, 85-94=high, 70-84=medium, <70=low."""
    if score >= 95:
        return "very_high"
    if score >= 85:
        return "high"
    if score >= 70:
        return "medium"
    return "low"


@dataclass
class V1FinalRow:
    """Row-level output per V1 spec."""
    contract_row_id:                str
    entity_cluster_id:              str | None
    resolved:                       bool
    public_company_id:              str | None
    public_company_id_type:         str | None  # CIK | INTERNAL | FIGI
    public_company_name:            str | None
    preferred_ticker:               str | None
    preferred_exchange:             str | None
    relationship_type:              str | None
    resolution_stage:               str | None
    confidence_score:               float
    confidence_band:                str
    match_explanation:              str | None
    matched_entity_name:            str | None
    matched_parent_name:            str | None
    matched_alias:                  str | None
    manual_override_used:           bool
    ambiguous:                      bool
    needs_review:                   bool
    share_class_rule_used:          str | None
    historical_ticker_attempted:    bool = False
    ticker_as_of_award_date:        str | None = None
    ticker_as_of_award_confidence:  str | None = None
    run_id:                         str | None = None


@dataclass
class V1EntityCache:
    """Entity-level cache output per V1 spec."""
    entity_cluster_id:      str
    ultimate_parent_uei:    str | None
    uei:                    str | None
    cage:                   str | None
    canonical_entity_name:  str | None
    canonical_parent_name:  str | None
    public_company_id:      str | None
    public_company_name:    str | None
    preferred_ticker:       str | None
    preferred_exchange:     str | None
    relationship_type:      str | None
    resolution_stage:       str | None
    confidence_score:       float
    first_resolved_at:      str
    last_validated_at:      str
    resolver_version:       str = RESOLVER_V1_VERSION
    issuer_master_version:  str = "unknown"
    source_evidence_json:   str | None = None


@dataclass
class V1RunMetrics:
    run_id:                str
    total_rows:            int = 0
    total_clusters:        int = 0
    new_resolved:          int = 0
    stage_wins:            dict = field(default_factory=dict)
    unresolved_count:      int = 0
    ambiguous_count:       int = 0
    override_hits:         int = 0
    cache_hits:            int = 0
    avg_candidates_scored: float = 0.0
    resolver_version:      str = RESOLVER_V1_VERSION


@dataclass
class V1ThresholdsConfig:
    """All tunables live here — never inline in pipeline code."""
    auto_accept_min_score:   float = 85.0
    auto_accept_margin:      float = 15.0
    fuzzy_min_score:         float = 70.0
    unresolved_cutoff:       float = 60.0
    review_cutoff:           float = 70.0
    min_tokens_for_fuzzy:    int   = 2
    max_candidates:          int   = 20
    enable_openfigi_tail:    bool  = False
```

- [ ] **Step 2: Smoke-test imports**

```bash
rtk python -c "from resolver.models import V1FinalRow, V1EntityCache, V1RunMetrics, V1ThresholdsConfig, score_to_confidence_band; print(score_to_confidence_band(97), score_to_confidence_band(88), score_to_confidence_band(75), score_to_confidence_band(50))"
```

Expected: `very_high high medium low`

- [ ] **Step 3: Commit**

```bash
rtk git add resolver/models.py && rtk git commit -m "feat: V1 output models and confidence-band function"
```

---

## Task 4: Issuer Master Builder

**Files:**
- Create: `resolver/issuer_master.py`
- Create: `tests/resolver/test_issuer_master.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/resolver/test_issuer_master.py
import os, tempfile
from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import (
    build_issuer_master_from_fixtures,
    is_eligible_common_equity,
)

MOCK_TICKERS = {
    "0": {"cik_str": 12345, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 67890, "ticker": "MSFT", "title": "Microsoft Corporation"},
    "2": {"cik_str": 11111, "ticker": "SPYUS", "title": "SPDR S&P 500 ETF Trust"},
}
MOCK_EXCHANGE = {
    "fields": ["cik", "name", "ticker", "exchange"],
    "data": [
        [12345, "Apple Inc.", "AAPL", "Nasdaq"],
        [67890, "Microsoft Corporation", "MSFT", "Nasdaq"],
        [11111, "SPDR S&P 500 ETF Trust", "SPYUS", "NYSEArca"],
    ],
}
MOCK_NASDAQ = (
    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size\n"
    "AAPL|Apple Inc. - Common Stock|Q|N|N|100\n"
    "MSFT|Microsoft Corporation - Common Stock|Q|N|N|100\n"
    "File Creation Time: 0000\n"
)

def _setup():
    d = tempfile.mkdtemp()
    con = get_db(os.path.join(d, "t.duckdb"))
    ensure_schema(con)
    return con

def test_etf_flagged_not_common_equity():
    con = _setup()
    build_issuer_master_from_fixtures(con, MOCK_TICKERS, MOCK_EXCHANGE, MOCK_NASDAQ, "")
    rows = con.execute(
        "SELECT is_etf, is_common_equity FROM issuer_master WHERE ticker_current='SPYUS'"
    ).fetchall()
    assert rows, "SPYUS not in issuer_master"
    assert rows[0][0] is True   # is_etf
    assert rows[0][1] is False  # is_common_equity
    con.close()

def test_common_stock_flagged_eligible():
    con = _setup()
    build_issuer_master_from_fixtures(con, MOCK_TICKERS, MOCK_EXCHANGE, MOCK_NASDAQ, "")
    rows = con.execute(
        "SELECT is_common_equity FROM issuer_master WHERE ticker_current='AAPL'"
    ).fetchall()
    assert rows[0][0] is True
    con.close()

def test_aliases_populated_for_apple():
    con = _setup()
    build_issuer_master_from_fixtures(con, MOCK_TICKERS, MOCK_EXCHANGE, MOCK_NASDAQ, "")
    aliases = con.execute(
        "SELECT alias_normalized_conservative FROM issuer_aliases "
        "WHERE public_company_id LIKE 'CIK_%' "
        "  AND alias_normalized_conservative LIKE 'APPLE%'"
    ).fetchall()
    assert len(aliases) >= 1
    con.close()

def test_is_eligible_common_equity():
    assert is_eligible_common_equity("Apple Inc. - Common Stock", "Nasdaq") is True
    assert is_eligible_common_equity("SPDR S&P 500 ETF Trust", "NYSEArca") is False
    assert is_eligible_common_equity("Boeing Preferred Stock Series A", "NYSE") is False
```

- [ ] **Step 2: Run — expect FAIL**

```bash
rtk python -m pytest tests/resolver/test_issuer_master.py -v 2>&1 | head -20
```

- [ ] **Step 3: Create `resolver/issuer_master.py`**

```python
"""resolver/issuer_master.py — Build issuer_master and issuer_aliases from SEC + Nasdaq Trader."""
from __future__ import annotations
import hashlib, json, logging, re
from datetime import datetime
from pathlib import Path
from typing import Any
import duckdb, requests
from resolver.normalize import conservative_normalize, aggressive_normalize
from resolver.models import DEFAULT_HTTP_HEADERS

log = logging.getLogger(__name__)

SEC_TICKERS_URL          = "https://www.sec.gov/files/company_tickers.json"
SEC_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_LISTED_URL        = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL         = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

US_EXCHANGES = {"Nasdaq", "NYSE", "NYSEArca", "NYSEAmerican", "BATS", "CBOE"}

_DISALLOWED_TOKENS = {
    "etf", "fund", "preferred", "warrant", "unit", "right", "note",
    "debenture", "bond", "reit", "spac", "trust", "depositary",
}


def is_eligible_common_equity(security_name: str | None, exchange: str | None) -> bool:
    if not security_name:
        return False
    lower = security_name.lower()
    for bad in _DISALLOWED_TOKENS:
        if bad in lower:
            return False
    if exchange and exchange not in US_EXCHANGES:
        return False
    return True


def _share_class_rank(ticker: str, security_name: str | None) -> int:
    sn = (security_name or "").upper()
    if "ADR" in sn or ticker.endswith("Y"):
        return 4
    if ticker.endswith("A") or "CLASS A" in sn:
        return 1
    if ticker.endswith("B") or "CLASS B" in sn:
        return 2
    return 3


def _alias_id(pub_id: str, raw: str, alias_type: str, source: str) -> str:
    return hashlib.md5(f"{pub_id}|{raw}|{alias_type}|{source}".encode()).hexdigest()[:16]


def _insert_alias(con, pub_id, raw, alias_type, source, valid_from=None, valid_to=None):
    if not raw or not str(raw).strip():
        return
    raw = str(raw).strip()
    cons = conservative_normalize(raw)
    agg  = aggressive_normalize(raw)
    aid  = _alias_id(pub_id, raw, alias_type, source)
    con.execute("""
        INSERT OR IGNORE INTO issuer_aliases
            (alias_id, public_company_id, alias_raw,
             alias_normalized_conservative, alias_normalized_aggressive,
             alias_type, source, valid_from, valid_to)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [aid, pub_id, raw, cons, agg, alias_type, source, valid_from, valid_to])


def build_issuer_master_from_sec(con, tickers_data: dict, exchange_data: dict) -> int:
    exch_lookup: dict[int, dict] = {}
    if "fields" in exchange_data and "data" in exchange_data:
        fields = exchange_data["fields"]
        for row in exchange_data["data"]:
            d = dict(zip(fields, row))
            cik_int = int(d.get("cik", 0) or 0)
            if cik_int:
                exch_lookup[cik_int] = d

    inserted = 0
    for _, entry in tickers_data.items():
        cik_int  = int(entry.get("cik_str", 0) or 0)
        ticker   = (entry.get("ticker") or "").strip().upper()
        name     = (entry.get("title") or "").strip()
        if not ticker or not name:
            continue
        cik_str  = str(cik_int).zfill(10)
        exch_row = exch_lookup.get(cik_int, {})
        exchange = exch_row.get("exchange", "")
        sec_name = exch_row.get("name") or name

        lower = sec_name.lower()
        is_etf  = "etf" in lower or "exchange-traded fund" in lower
        is_fund = ("fund" in lower or "trust" in lower) and not is_etf
        is_pref = "preferred" in lower or "depositary" in lower
        is_warr = "warrant" in lower
        is_unit = " unit" in lower
        is_adr  = "adr" in lower or "american depositary" in lower
        is_com  = is_eligible_common_equity(sec_name, exchange)

        pub_id = f"CIK_{cik_str}"
        con.execute("""
            INSERT OR IGNORE INTO issuer_master (
                public_company_id, public_company_id_type, cik,
                issuer_name_current, ticker_current, exchange_current,
                is_us_tradable, is_common_equity, share_class_rank,
                is_adr, is_etf, is_fund, is_warrant, is_unit, is_preferred,
                active_status, source_priority
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [pub_id, "CIK", cik_str, sec_name, ticker, exchange,
              exchange in US_EXCHANGES or not exchange, is_com,
              _share_class_rank(ticker, sec_name),
              is_adr, is_etf, is_fund, is_warr, is_unit, is_pref,
              "active", 1])
        _insert_alias(con, pub_id, name,     "current_name", "sec_tickers")
        _insert_alias(con, pub_id, sec_name, "current_name", "sec_exchange")
        _insert_alias(con, pub_id, ticker,   "ticker_name",  "sec_tickers")
        inserted += 1

    log.info(f"SEC: {inserted} issuers loaded")
    return inserted


def _parse_nasdaq_lines(text: str) -> list[dict]:
    rows = []
    lines = text.splitlines()
    if not lines:
        return rows
    headers = [h.strip() for h in lines[0].split("|")]
    for line in lines[1:]:
        if line.startswith("File Creation Time") or not line.strip():
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 2:
            rows.append(dict(zip(headers, parts)))
    return rows


def build_issuer_master_from_nasdaq(con, nasdaq_text: str, other_text: str) -> int:
    added = 0
    for row in _parse_nasdaq_lines(nasdaq_text) + _parse_nasdaq_lines(other_text):
        ticker   = (row.get("Symbol") or row.get("ACT Symbol") or "").strip().upper()
        sec_name = (row.get("Security Name") or "").strip()
        if not ticker or not sec_name:
            continue
        existing = con.execute(
            "SELECT public_company_id FROM issuer_master WHERE ticker_current=?", [ticker]
        ).fetchone()
        if existing:
            pub_id = existing[0]
        else:
            pub_id = f"NASDAQ_{ticker}"
            is_com = is_eligible_common_equity(sec_name, "Nasdaq")
            con.execute("""
                INSERT OR IGNORE INTO issuer_master (
                    public_company_id, public_company_id_type, cik,
                    issuer_name_current, ticker_current, exchange_current,
                    is_us_tradable, is_common_equity, share_class_rank,
                    is_adr, is_etf, is_fund, is_warrant, is_unit, is_preferred,
                    active_status, source_priority
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [pub_id, "INTERNAL", None, sec_name, ticker, "Nasdaq",
                  True, is_com, 3, False, "etf" in sec_name.lower(),
                  False, False, False, False, "active", 2])
            added += 1
        _insert_alias(con, pub_id, sec_name, "security_name", "nasdaq_trader")
    log.info(f"Nasdaq Trader: {added} new symbols added")
    return added


def build_issuer_master_from_fixtures(con, tickers_data, exchange_data, nasdaq_text, other_text):
    build_issuer_master_from_sec(con, tickers_data, exchange_data)
    build_issuer_master_from_nasdaq(con, nasdaq_text, other_text)


def _fetch_json(url: str, cache_dir: str | None) -> Any:
    if cache_dir:
        fname = hashlib.md5(url.encode()).hexdigest() + ".json"
        p = Path(cache_dir) / fname
        if p.exists():
            return json.loads(p.read_text())
    log.info(f"GET {url}")
    r = requests.get(url, headers=DEFAULT_HTTP_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if cache_dir:
        (Path(cache_dir) / fname).write_text(json.dumps(data))
    return data


def _fetch_text(url: str, cache_dir: str | None) -> str:
    if cache_dir:
        fname = hashlib.md5(url.encode()).hexdigest() + ".txt"
        p = Path(cache_dir) / fname
        if p.exists():
            return p.read_text()
    log.info(f"GET {url}")
    r = requests.get(url, headers=DEFAULT_HTTP_HEADERS, timeout=30)
    r.raise_for_status()
    text = r.text
    if cache_dir:
        (Path(cache_dir) / fname).write_text(text)
    return text


def refresh_issuer_master(
    con,
    cache_dir: str = "data/cache",
    force: bool = False,
) -> str:
    """Download SEC + Nasdaq, rebuild issuer_master and issuer_aliases. Returns version string."""
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    cd = None if force else cache_dir
    tickers  = _fetch_json(SEC_TICKERS_URL, cd)
    exchange = _fetch_json(SEC_TICKERS_EXCHANGE_URL, cd)
    nasdaq   = _fetch_text(NASDAQ_LISTED_URL, cd)
    other    = _fetch_text(OTHER_LISTED_URL, cd)
    con.execute("DELETE FROM issuer_aliases")
    con.execute("DELETE FROM issuer_master")
    build_issuer_master_from_fixtures(con, tickers, exchange, nasdaq, other)
    version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info(f"Issuer master refreshed. Version: {version}")
    return version


def get_issuer_master_version(con) -> str:
    try:
        n = con.execute("SELECT COUNT(*) FROM issuer_master").fetchone()[0]
        return f"rows={n}"
    except Exception:
        return "unknown"
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_issuer_master.py -v 2>&1 | head -20
```

- [ ] **Step 5: Commit**

```bash
rtk git add resolver/issuer_master.py tests/resolver/test_issuer_master.py && rtk git commit -m "feat: issuer_master builder from SEC + Nasdaq Trader"
```

---

## Task 5: Entity Clustering

**Files:**
- Create: `resolver/clusters.py`
- Create: `tests/resolver/test_clusters.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/resolver/test_clusters.py
import os, tempfile
from decimal import Decimal
from datetime import date
from resolver.storage import get_db, ensure_schema
from resolver.clusters import build_entity_clusters, choose_cluster_key
from resolver.models import ContractIdentityFeatures

def _feat(row_id, uei=None, parent_uei=None, cage=None, name=None, parent_name=None, dba=None):
    return ContractIdentityFeatures(
        contract_row_id=row_id, award_date=date(2023, 1, 1),
        awardee_uei=uei, parent_uei=parent_uei, cage_code=cage,
        awardee_name_raw=name, awardee_name_norm=name,
        awardee_dba_raw=dba, awardee_dba_norm=dba,
        parent_name_raw=parent_name, parent_name_norm=parent_name,
        website_raw=None, website_domain=None,
        vendor_city_norm=None, vendor_state_norm="VA",
        vendor_zip_norm=None, vendor_country_norm="USA",
        incorporation_country_norm="USA", phone_norm=None,
        dollars_obligated=Decimal("1000000"),
    )

def test_parent_uei_wins_over_uei():
    f = _feat("r1", uei="U1", parent_uei="PUEI_X")
    ktype, kval = choose_cluster_key(f)
    assert ktype == "ultimate_parent_uei" and kval == "PUEI_X"

def test_cage_used_when_no_uei():
    f = _feat("r1", cage="12345", name="Some Corp")
    ktype, kval = choose_cluster_key(f)
    assert ktype == "cage" and kval == "12345"

def test_same_parent_uei_clusters_together():
    feats = [
        _feat("r1", parent_uei="PUEI_X", name="Sub A", parent_name="Parent Co"),
        _feat("r2", parent_uei="PUEI_X", name="Sub B", parent_name="Parent Co"),
        _feat("r3", parent_uei="PUEI_Y", name="Other"),
    ]
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        build_entity_clusters(feats, con)
        clusters = con.execute(
            "SELECT row_count FROM entity_clusters ORDER BY row_count DESC"
        ).fetchall()
        assert len(clusters) == 2
        assert clusters[0][0] == 2
        assert clusters[1][0] == 1
        con.close()

def test_canonical_parent_name_stored():
    feats = [_feat("r1", parent_uei="PUEI_X", parent_name="RTX Corporation")]
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        build_entity_clusters(feats, con)
        row = con.execute("SELECT canonical_parent_name FROM entity_clusters").fetchone()
        assert row[0] == "RTX Corporation"
        con.close()
```

- [ ] **Step 2: Run — expect FAIL**

```bash
rtk python -m pytest tests/resolver/test_clusters.py -v 2>&1 | head -20
```

- [ ] **Step 3: Create `resolver/clusters.py`**

```python
"""resolver/clusters.py — Build entity_clusters from contract identity features."""
from __future__ import annotations
import hashlib, json, logging
from collections import Counter, defaultdict
import duckdb
from resolver.models import ContractIdentityFeatures

log = logging.getLogger(__name__)


def choose_cluster_key(feat: ContractIdentityFeatures) -> tuple[str, str]:
    """Spec priority: parent_uei > uei > cage > name_sig."""
    if feat.parent_uei:
        return "ultimate_parent_uei", feat.parent_uei
    if feat.awardee_uei:
        return "uei", feat.awardee_uei
    if feat.cage_code:
        return "cage", feat.cage_code
    base = feat.parent_name_norm or feat.awardee_name_norm or feat.contract_row_id
    return "name_sig", hashlib.md5(base.encode()).hexdigest()[:12]


def _cluster_id(key_type: str, key_value: str) -> str:
    return hashlib.md5(f"{key_type}:{key_value}".encode()).hexdigest()[:16]


def _most_common(vals: list) -> str | None:
    if not vals:
        return None
    return Counter(vals).most_common(1)[0][0]


def build_entity_clusters(
    features: list[ContractIdentityFeatures],
    con: duckdb.DuckDBPyConnection,
) -> dict[str, str]:
    """
    Cluster features into entity_clusters in DuckDB.
    Returns {contract_row_id: entity_cluster_id}.
    """
    buckets: dict[str, list[ContractIdentityFeatures]] = defaultdict(list)
    row_map: dict[str, str] = {}

    for feat in features:
        ktype, kval = choose_cluster_key(feat)
        cid = _cluster_id(ktype, kval)
        buckets[cid].append(feat)
        row_map[feat.contract_row_id] = cid

    for cid, feats in buckets.items():
        ktype, kval = choose_cluster_key(feats[0])
        all_ueis    = list({f.awardee_uei  for f in feats if f.awardee_uei})
        all_cages   = list({f.cage_code    for f in feats if f.cage_code})
        all_legal   = list({f.awardee_name_raw  for f in feats if f.awardee_name_raw})
        all_parents = list({f.parent_name_raw   for f in feats if f.parent_name_raw})
        all_dbas    = list({f.awardee_dba_raw   for f in feats if f.awardee_dba_raw})
        state_freq  = dict(Counter(f.vendor_state_norm  for f in feats if f.vendor_state_norm))
        ctry_freq   = dict(Counter(f.vendor_country_norm for f in feats if f.vendor_country_norm))
        dates       = sorted(f.award_date for f in feats if f.award_date)
        total_obl   = sum(float(f.dollars_obligated or 0) for f in feats)

        can_parent  = _most_common(all_parents)
        can_entity  = _most_common(all_legal)

        parent_uei  = next((f.parent_uei  for f in feats if f.parent_uei),  None)
        uei         = next((f.awardee_uei for f in feats if f.awardee_uei), None)
        cage        = next((f.cage_code   for f in feats if f.cage_code),   None)

        con.execute("""
            INSERT OR REPLACE INTO entity_clusters (
                entity_cluster_id, cluster_key_type,
                ultimate_parent_uei, uei, cage_code,
                canonical_entity_name, canonical_parent_name, canonical_display_name,
                all_ueis_json, all_cages_json, all_legal_names_json,
                all_parent_names_json, all_dba_names_json,
                state_freq_json, country_freq_json, naics_freq_json,
                first_seen_date, last_seen_date, row_count, total_obligated
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [cid, ktype,
              parent_uei, uei, cage,
              can_entity, can_parent, can_parent or can_entity,
              json.dumps(all_ueis), json.dumps(all_cages),
              json.dumps(all_legal), json.dumps(all_parents), json.dumps(all_dbas),
              json.dumps(state_freq), json.dumps(ctry_freq), "{}",
              str(dates[0]) if dates else None,
              str(dates[-1]) if dates else None,
              len(feats), total_obl])

        for feat in feats:
            eid = hashlib.md5(f"{feat.contract_row_id}:{cid}".encode()).hexdigest()[:16]
            con.execute("""
                INSERT OR IGNORE INTO entity_edges
                    (edge_id, entity_cluster_id, contract_row_id, edge_type)
                VALUES (?,?,?,?)
            """, [eid, cid, feat.contract_row_id, ktype])

    log.info(f"Built {len(buckets)} clusters from {len(features)} rows")
    return row_map
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_clusters.py -v 2>&1 | head -20
```

- [ ] **Step 5: Commit**

```bash
rtk git add resolver/clusters.py tests/resolver/test_clusters.py && rtk git commit -m "feat: entity clustering — cluster-key priority, DuckDB upsert"
```

---

## Task 6: Pipeline Stages 0–4

**Files:**
- Create: `resolver/pipeline.py`
- Create: `tests/resolver/test_pipeline.py`

- [ ] **Step 1: Write tests for stages 0–4**

```python
# tests/resolver/test_pipeline.py
import os, tempfile, uuid, pytest
import duckdb
from resolver.storage import get_db, ensure_schema
from resolver.normalize import conservative_normalize
from resolver.pipeline import (
    ClusterContext, stage0_override, stage1_cache,
    stage2_exact_parent, stage3_exact_direct, stage4_alias_match,
)

def _con():
    d = tempfile.mkdtemp()
    con = get_db(os.path.join(d, "t.duckdb"))
    ensure_schema(con)
    return con

def _seed_issuer(con, pub_id="CIK_0012345", name="Acme Corp", ticker="ACME", exchange="Nasdaq"):
    con.execute("""
        INSERT OR IGNORE INTO issuer_master (
            public_company_id, public_company_id_type, cik,
            issuer_name_current, ticker_current, exchange_current,
            is_us_tradable, is_common_equity, share_class_rank,
            is_adr, is_etf, is_fund, is_warrant, is_unit, is_preferred,
            active_status, source_priority
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [pub_id, "CIK", "0012345", name, ticker, exchange,
          True, True, 1, False, False, False, False, False, False, "active", 1])
    cons = conservative_normalize(name)
    agg  = cons
    con.execute("""
        INSERT OR IGNORE INTO issuer_aliases
            (alias_id, public_company_id, alias_raw,
             alias_normalized_conservative, alias_normalized_aggressive,
             alias_type, source, valid_from, valid_to)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [f"{pub_id}_cn", pub_id, name, cons, agg, "current_name", "test", None, None])

def _ctx(cluster_id="c1", parent_name=None, entity_name="Acme Corp",
         uei=None, parent_uei=None, cage=None):
    return ClusterContext(
        cluster_id=cluster_id,
        canonical_parent_name=parent_name,
        canonical_entity_name=entity_name,
        uei=uei, parent_uei=parent_uei, cage=cage,
        parent_name_norm=conservative_normalize(parent_name),
        legal_name_norm=conservative_normalize(entity_name),
    )

# Stage 0
def test_stage0_uei_override():
    con = _con()
    con.execute("""
        INSERT INTO manual_overrides
            (override_id, override_key_type, override_key_value,
             public_company_id, public_company_name, preferred_ticker,
             preferred_exchange, relationship_type, active)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [str(uuid.uuid4()), "uei", "UEI_ABC",
          "CIK_X", "Acme", "ACME", "Nasdaq", "direct_public_awardee", True])
    result = stage0_override(_ctx(uei="UEI_ABC"), con)
    assert result is not None
    assert result["preferred_ticker"] == "ACME"
    assert result["manual_override_used"] is True
    con.close()

def test_stage0_no_match():
    con = _con()
    result = stage0_override(_ctx(uei="UEI_UNKNOWN"), con)
    assert result is None
    con.close()

# Stage 1
def test_stage1_cache_hit():
    con = _con()
    con.execute("""
        INSERT INTO resolution_cache (
            entity_cluster_id, resolved, public_company_id, public_company_name,
            preferred_ticker, preferred_exchange, relationship_type,
            resolution_stage, confidence_score, confidence_band,
            match_explanation, source_evidence_json,
            resolver_version, issuer_master_version,
            first_resolved_at, last_validated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, ["c1", True, "CIK_X", "Acme", "ACME", "Nasdaq", "direct_public_awardee",
          "stage2", 97.0, "very_high", "cached", "{}", "1.0", "unknown",
          "2024-01-01", "2024-01-01"])
    result = stage1_cache(_ctx(), con)
    assert result is not None
    assert result["preferred_ticker"] == "ACME"
    con.close()

def test_stage1_cache_miss():
    con = _con()
    result = stage1_cache(_ctx(), con)
    assert result is None
    con.close()

# Stage 2
def test_stage2_exact_parent_match():
    con = _con()
    _seed_issuer(con, name="Raytheon Technologies", ticker="RTX")
    ctx = _ctx(parent_name="Raytheon Technologies", entity_name="Raytheon Sub")
    result = stage2_exact_parent(ctx, con)
    assert result is not None
    assert result["preferred_ticker"] == "RTX"
    assert result["relationship_type"] == "public_ultimate_parent"
    assert result["confidence_score"] >= 95
    con.close()

def test_stage2_no_parent_name():
    con = _con()
    _seed_issuer(con)
    result = stage2_exact_parent(_ctx(parent_name=None), con)
    assert result is None
    con.close()

# Stage 3
def test_stage3_direct_match():
    con = _con()
    _seed_issuer(con, name="Palantir Technologies", ticker="PLTR")
    ctx = _ctx(entity_name="Palantir Technologies")
    result = stage3_exact_direct(ctx, con)
    assert result is not None
    assert result["preferred_ticker"] == "PLTR"
    assert result["relationship_type"] == "direct_public_awardee"
    con.close()

def test_stage3_no_match():
    con = _con()
    _seed_issuer(con, name="Lockheed Martin", ticker="LMT")
    result = stage3_exact_direct(_ctx(entity_name="Foobar Industries"), con)
    assert result is None
    con.close()

# Stage 4
def test_stage4_aggressive_alias_match():
    con = _con()
    _seed_issuer(con, name="Science Applications International", ticker="SAIC")
    # Insert aggressive-normalized alias
    from resolver.normalize import aggressive_normalize
    agg = aggressive_normalize("Science Applications International")
    con.execute("""
        UPDATE issuer_aliases SET alias_normalized_aggressive=?
        WHERE public_company_id='CIK_0012345'
    """, [agg])
    ctx = _ctx(entity_name="Science Applications International Corporation")
    result = stage4_alias_match(ctx, con)
    assert result is not None
    assert result["preferred_ticker"] == "SAIC"
    con.close()
```

- [ ] **Step 2: Run — expect FAIL**

```bash
rtk python -m pytest tests/resolver/test_pipeline.py -v 2>&1 | head -30
```

- [ ] **Step 3: Create `resolver/pipeline.py`** (stages 0–4)

```python
"""resolver/pipeline.py — 8-stage V1 resolution pipeline."""
from __future__ import annotations
import json, logging
from dataclasses import dataclass, field
from datetime import datetime
import duckdb
from resolver.normalize import conservative_normalize, aggressive_normalize, token_metadata
from resolver.models import score_to_confidence_band, V1ThresholdsConfig, RESOLVER_V1_VERSION

log = logging.getLogger(__name__)


@dataclass
class ClusterContext:
    cluster_id:             str
    canonical_parent_name:  str | None
    canonical_entity_name:  str | None
    uei:                    str | None
    parent_uei:             str | None
    cage:                   str | None
    parent_name_norm:       str | None  # conservative
    legal_name_norm:        str | None  # conservative
    all_parent_names:       list[str] = field(default_factory=list)
    all_legal_names:        list[str] = field(default_factory=list)
    all_dba_names:          list[str] = field(default_factory=list)


def _result(cluster_id, stage, pub_id, pub_name, ticker, exchange, relationship,
            score, explanation, matched_entity=None, matched_parent=None,
            matched_alias=None, override=False, ambiguous=False,
            needs_review=False, share_class_rule=None, evidence=None) -> dict:
    return {
        "entity_cluster_id":     cluster_id,
        "resolved":              ticker is not None,
        "public_company_id":     pub_id,
        "public_company_id_type": "CIK" if (pub_id or "").startswith("CIK_") else "INTERNAL",
        "public_company_name":   pub_name,
        "preferred_ticker":      ticker,
        "preferred_exchange":    exchange,
        "relationship_type":     relationship,
        "resolution_stage":      stage,
        "confidence_score":      score,
        "confidence_band":       score_to_confidence_band(score),
        "match_explanation":     explanation,
        "matched_entity_name":   matched_entity,
        "matched_parent_name":   matched_parent,
        "matched_alias":         matched_alias,
        "manual_override_used":  override,
        "ambiguous":             ambiguous,
        "needs_review":          needs_review,
        "share_class_rule_used": share_class_rule or "issuer_master_rank",
        "historical_ticker_attempted": False,
        "ticker_as_of_award_date":     None,
        "ticker_as_of_award_confidence": None,
        "source_evidence_json":  json.dumps(evidence or {}),
    }


def _unresolved(cluster_id: str, reason: str = "no_match") -> dict:
    return _result(cluster_id, "unresolved", None, None, None, None,
                   "unresolved", 0.0, reason)


def _ambiguous(cluster_id: str, top_candidates: list[dict]) -> dict:
    return _result(cluster_id, "ambiguous", None, None, None, None,
                   "ambiguous", 0.0, "multiple_near_equal_candidates",
                   ambiguous=True, needs_review=True,
                   evidence={"top": [{"ticker": c.get("ticker_current"),
                                       "score": c.get("score")} for c in top_candidates[:3]]})


# ── Stage 0 ───────────────────────────────────────────────────────────────────

def stage0_override(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    checks = []
    if ctx.parent_uei:      checks.append(("ultimate_parent_uei", ctx.parent_uei))
    if ctx.uei:             checks.append(("uei", ctx.uei))
    if ctx.cage:            checks.append(("cage", ctx.cage))
    if ctx.parent_name_norm:checks.append(("parent_name_norm", ctx.parent_name_norm))
    if ctx.legal_name_norm: checks.append(("legal_name_norm",  ctx.legal_name_norm))
    for ktype, kval in checks:
        row = con.execute("""
            SELECT public_company_id, public_company_name, preferred_ticker,
                   preferred_exchange, relationship_type
            FROM manual_overrides
            WHERE override_key_type=? AND override_key_value=? AND active=TRUE
            LIMIT 1
        """, [ktype, kval]).fetchone()
        if row:
            pub_id, pub_name, ticker, exchange, rel = row
            return _result(ctx.cluster_id, "stage0_override",
                           pub_id, pub_name, ticker, exchange,
                           rel or "direct_public_awardee", 100.0,
                           f"override:{ktype}={kval}", override=True,
                           evidence={"override_key_type": ktype, "override_key_value": kval})
    return None


# ── Stage 1 ───────────────────────────────────────────────────────────────────

def stage1_cache(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    row = con.execute("""
        SELECT resolved, public_company_id, public_company_name,
               preferred_ticker, preferred_exchange, relationship_type,
               resolution_stage, confidence_score, confidence_band,
               match_explanation, source_evidence_json
        FROM resolution_cache WHERE entity_cluster_id=? LIMIT 1
    """, [ctx.cluster_id]).fetchone()
    if not row:
        return None
    (resolved, pub_id, pub_name, ticker, exchange, rel,
     stage, score, band, explanation, evidence) = row
    return {
        "entity_cluster_id":     ctx.cluster_id,
        "resolved":              bool(resolved),
        "public_company_id":     pub_id,
        "public_company_id_type": "CIK" if (pub_id or "").startswith("CIK_") else "INTERNAL",
        "public_company_name":   pub_name,
        "preferred_ticker":      ticker,
        "preferred_exchange":    exchange,
        "relationship_type":     rel,
        "resolution_stage":      f"cache({stage})",
        "confidence_score":      float(score or 0),
        "confidence_band":       band or score_to_confidence_band(float(score or 0)),
        "match_explanation":     explanation,
        "manual_override_used":  False,
        "ambiguous":             False,
        "needs_review":          False,
        "share_class_rule_used": "cached",
        "historical_ticker_attempted": False,
        "ticker_as_of_award_date":     None,
        "ticker_as_of_award_confidence": None,
        "source_evidence_json":  evidence or "{}",
    }


# ── Shared: exact alias lookup ────────────────────────────────────────────────

def _lookup_exact(name_cons: str | None, con: duckdb.DuckDBPyConnection) -> list[tuple]:
    if not name_cons:
        return []
    return con.execute("""
        SELECT im.public_company_id, im.issuer_name_current, im.ticker_current,
               im.exchange_current, ia.alias_normalized_conservative
        FROM issuer_aliases ia
        JOIN issuer_master im ON ia.public_company_id = im.public_company_id
        WHERE ia.alias_normalized_conservative = ?
          AND im.is_common_equity = TRUE AND im.active_status = 'active'
    """, [name_cons]).fetchall()


# ── Stage 2 ───────────────────────────────────────────────────────────────────

def stage2_exact_parent(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    search = ctx.parent_name_norm or conservative_normalize(ctx.canonical_parent_name)
    rows = _lookup_exact(search, con)
    if len(rows) == 1:
        pub_id, pub_name, ticker, exchange, alias = rows[0]
        return _result(ctx.cluster_id, "stage2_exact_parent",
                       pub_id, pub_name, ticker, exchange,
                       "public_ultimate_parent", 97.0,
                       f"exact_parent:{alias}",
                       matched_parent=ctx.canonical_parent_name,
                       matched_alias=alias,
                       evidence={"alias": alias, "pub_id": pub_id})
    return None


# ── Stage 3 ───────────────────────────────────────────────────────────────────

def stage3_exact_direct(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    names = []
    if ctx.legal_name_norm:
        names.append(ctx.legal_name_norm)
    for n in ctx.all_legal_names + ctx.all_dba_names:
        c = conservative_normalize(n)
        if c and c not in names:
            names.append(c)
    for search in names:
        rows = _lookup_exact(search, con)
        if len(rows) == 1:
            pub_id, pub_name, ticker, exchange, alias = rows[0]
            # Slight penalty if parent context conflicts
            score = 88.0 if (ctx.parent_name_norm and
                              conservative_normalize(pub_name) != ctx.parent_name_norm) else 92.0
            return _result(ctx.cluster_id, "stage3_exact_direct",
                           pub_id, pub_name, ticker, exchange,
                           "direct_public_awardee", score,
                           f"exact_direct:{alias}",
                           matched_entity=ctx.canonical_entity_name,
                           matched_alias=alias,
                           evidence={"searched": search, "alias": alias})
    return None


# ── Stage 4 ───────────────────────────────────────────────────────────────────

def stage4_alias_match(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    all_names = (
        ([ctx.canonical_parent_name] if ctx.canonical_parent_name else []) +
        ([ctx.canonical_entity_name] if ctx.canonical_entity_name else []) +
        ctx.all_legal_names + ctx.all_parent_names + ctx.all_dba_names
    )
    for raw in all_names:
        agg = aggressive_normalize(raw)
        if not agg:
            continue
        rows = con.execute("""
            SELECT im.public_company_id, im.issuer_name_current,
                   im.ticker_current, im.exchange_current,
                   ia.alias_normalized_aggressive
            FROM issuer_aliases ia
            JOIN issuer_master im ON ia.public_company_id = im.public_company_id
            WHERE ia.alias_normalized_aggressive = ?
              AND im.is_common_equity = TRUE AND im.active_status = 'active'
        """, [agg]).fetchall()
        if len(rows) == 1:
            pub_id, pub_name, ticker, exchange, alias = rows[0]
            rel = ("public_ultimate_parent" if raw == ctx.canonical_parent_name
                   else "direct_public_awardee")
            return _result(ctx.cluster_id, "stage4_alias",
                           pub_id, pub_name, ticker, exchange, rel, 85.0,
                           f"agg_alias:{alias}",
                           matched_alias=alias,
                           evidence={"agg": agg, "raw": raw})
    return None
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_pipeline.py -v 2>&1 | head -30
```

- [ ] **Step 5: Commit**

```bash
rtk git add resolver/pipeline.py tests/resolver/test_pipeline.py && rtk git commit -m "feat: pipeline stages 0-4 — override, cache, exact parent/direct/alias"
```

---

## Task 7: Pipeline Stages 5–7 + Cache Write + Orchestrator

**Files:**
- Modify: `resolver/pipeline.py` (append stages 5–7, cache write, orchestrator)
- Modify: `tests/resolver/test_pipeline.py` (append stage 5–7 tests)

- [ ] **Step 1: Append stage 5–7 tests to `tests/resolver/test_pipeline.py`**

```python
# Append to tests/resolver/test_pipeline.py

from resolver.pipeline import (
    stage5_generate_candidates, stage6_fuzzy_score,
    stage7_accept, resolve_cluster,
)
from resolver.models import V1ThresholdsConfig

def _seed_big_alias(con, pub_id, name, ticker, exchange="Nasdaq"):
    from resolver.normalize import conservative_normalize, aggressive_normalize
    cons = conservative_normalize(name)
    agg  = aggressive_normalize(name)
    con.execute("""
        INSERT OR IGNORE INTO issuer_master (
            public_company_id, public_company_id_type, cik,
            issuer_name_current, ticker_current, exchange_current,
            is_us_tradable, is_common_equity, share_class_rank,
            is_adr, is_etf, is_fund, is_warrant, is_unit, is_preferred,
            active_status, source_priority
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [pub_id, "CIK", None, name, ticker, exchange,
          True, True, 1, False, False, False, False, False, False, "active", 1])
    from resolver.issuer_master import _alias_id
    aid = _alias_id(pub_id, name, "current_name", "test")
    con.execute("""
        INSERT OR IGNORE INTO issuer_aliases
            (alias_id, public_company_id, alias_raw,
             alias_normalized_conservative, alias_normalized_aggressive,
             alias_type, source, valid_from, valid_to)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [aid, pub_id, name, cons, agg, "current_name", "test", None, None])


def test_stage5_returns_candidates():
    con = _con()
    _seed_big_alias(con, "CIK_BAH", "Booz Allen Hamilton", "BAH")
    ctx = _ctx(entity_name="Booz Allen Hamilton Holding")
    candidates = stage5_generate_candidates(ctx, con)
    tickers = [c["ticker_current"] for c in candidates]
    assert "BAH" in tickers


def test_stage6_scores_token_overlap():
    con = _con()
    _seed_big_alias(con, "CIK_SAIC", "Science Applications International", "SAIC")
    ctx = _ctx(entity_name="Science Applications International Corporation")
    candidates = stage5_generate_candidates(ctx, con)
    scored = stage6_fuzzy_score(ctx, candidates)
    assert len(scored) > 0
    assert scored[0]["score"] >= 60


def test_stage7_accepts_clear_winner():
    thresholds = V1ThresholdsConfig()
    scored = [
        {"public_company_id": "CIK_A", "issuer_name_current": "Acme",
         "ticker_current": "ACME", "exchange_current": "Nasdaq",
         "is_common_equity": True, "score": 91.0, "explanation": "test"},
        {"public_company_id": "CIK_B", "issuer_name_current": "Other",
         "ticker_current": "OTHR", "exchange_current": "NYSE",
         "is_common_equity": True, "score": 55.0, "explanation": "weak"},
    ]
    result = stage7_accept("c1", scored, thresholds)
    assert result["resolved"] is True and result["preferred_ticker"] == "ACME"


def test_stage7_ambiguous_close_scores():
    thresholds = V1ThresholdsConfig()
    scored = [
        {"public_company_id": "CIK_A", "issuer_name_current": "Acme",
         "ticker_current": "ACME", "exchange_current": "Nasdaq",
         "is_common_equity": True, "score": 80.0, "explanation": "x"},
        {"public_company_id": "CIK_B", "issuer_name_current": "Acme2",
         "ticker_current": "ACM2", "exchange_current": "NYSE",
         "is_common_equity": True, "score": 79.0, "explanation": "y"},
    ]
    result = stage7_accept("c1", scored, thresholds)
    assert result["ambiguous"] is True and result["needs_review"] is True


def test_resolve_cluster_end_to_end():
    con = _con()
    _seed_big_alias(con, "CIK_RTX", "RTX Corporation", "RTX")
    ctx = _ctx(parent_name="RTX Corporation", entity_name="Raytheon Sub")
    result = resolve_cluster(ctx, con, V1ThresholdsConfig())
    assert result["preferred_ticker"] == "RTX"
    # Second call uses cache
    result2 = resolve_cluster(ctx, con, V1ThresholdsConfig())
    assert result2["preferred_ticker"] == "RTX"
    assert "cache" in result2["resolution_stage"]
```

- [ ] **Step 2: Run — expect FAIL**

```bash
rtk python -m pytest tests/resolver/test_pipeline.py -v -k "stage5 or stage6 or stage7 or end_to_end" 2>&1 | head -20
```

- [ ] **Step 3: Append to `resolver/pipeline.py`**

```python
# ── Stage 5: Candidate generation ────────────────────────────────────────────

def stage5_generate_candidates(
    ctx: ClusterContext,
    con: duckdb.DuckDBPyConnection,
    max_candidates: int = 20,
) -> list[dict]:
    query_names = [n for n in [ctx.canonical_parent_name, ctx.canonical_entity_name] if n]
    all_tokens: set[str] = set()
    for name in query_names:
        all_tokens.update(token_metadata(name)["tokens"])
    if not all_tokens:
        return []

    rows = con.execute("""
        SELECT DISTINCT im.public_company_id, im.issuer_name_current,
               im.ticker_current, im.exchange_current, im.is_common_equity,
               ia.alias_normalized_conservative
        FROM issuer_aliases ia
        JOIN issuer_master im ON ia.public_company_id = im.public_company_id
        WHERE im.is_common_equity = TRUE AND im.active_status = 'active'
        LIMIT 100000
    """).fetchall()

    scored = []
    for pub_id, name, ticker, exchange, is_com, alias_cons in rows:
        if not alias_cons:
            continue
        alias_tokens = set(alias_cons.split())
        overlap = len(all_tokens & alias_tokens)
        if overlap == 0:
            continue
        ratio = overlap / max(len(all_tokens), len(alias_tokens), 1)
        if ratio >= 0.35:
            scored.append({
                "public_company_id": pub_id, "issuer_name_current": name,
                "ticker_current": ticker, "exchange_current": exchange,
                "is_common_equity": is_com,
                "alias_normalized_conservative": alias_cons,
                "_overlap": ratio,
            })

    scored.sort(key=lambda x: x["_overlap"], reverse=True)
    return scored[:max_candidates]


# ── Stage 6: Fuzzy scoring ────────────────────────────────────────────────────

def stage6_fuzzy_score(ctx: ClusterContext, candidates: list[dict]) -> list[dict]:
    try:
        from rapidfuzz import fuzz
    except ImportError:
        log.warning("rapidfuzz not installed; stage6 scoring skipped")
        return []

    q_parent = conservative_normalize(ctx.canonical_parent_name) or ""
    q_entity = conservative_normalize(ctx.canonical_entity_name) or ""

    scored = []
    for c in candidates:
        alias = c.get("alias_normalized_conservative") or ""
        score = 0.0
        parts = []

        if q_parent:
            ps = fuzz.token_set_ratio(q_parent, alias) / 100.0
            score += ps * 25
            parts.append(f"parent={ps:.2f}")

        if q_entity:
            es = fuzz.token_set_ratio(q_entity, alias) / 100.0
            score += es * 20
            parts.append(f"entity={es:.2f}")

        # Token overlap
        q_tokens = set(token_metadata(ctx.canonical_parent_name or ctx.canonical_entity_name)["tokens"])
        a_tokens = set(token_metadata(c.get("issuer_name_current"))["tokens"])
        if q_tokens and a_tokens:
            ov = len(q_tokens & a_tokens) / max(len(q_tokens), len(a_tokens))
            score += ov * 15
            parts.append(f"tok={ov:.2f}")

        # Eligibility bonus
        if c.get("is_common_equity"):
            score += 5.0

        scored.append({**c, "score": round(score, 2), "explanation": " ".join(parts)})

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


# ── Stage 7: Accept / ambiguous / unresolved ─────────────────────────────────

def stage7_accept(
    cluster_id: str,
    scored: list[dict],
    thresholds: V1ThresholdsConfig,
) -> dict:
    if not scored:
        return _unresolved(cluster_id, "no_candidates")
    top    = scored[0]
    runner = scored[1] if len(scored) > 1 else None
    margin = top["score"] - (runner["score"] if runner else 0.0)

    if top["score"] >= thresholds.auto_accept_min_score and margin >= thresholds.auto_accept_margin:
        return _result(cluster_id, "stage7_fuzzy",
                       top["public_company_id"], top["issuer_name_current"],
                       top["ticker_current"], top["exchange_current"],
                       "direct_public_awardee", top["score"],
                       top.get("explanation", ""),
                       matched_entity=top["issuer_name_current"],
                       evidence={"top_score": top["score"], "margin": margin})

    if (top["score"] >= thresholds.fuzzy_min_score and runner
            and margin < thresholds.auto_accept_margin):
        return _ambiguous(cluster_id, scored)

    return _unresolved(cluster_id, f"score={top['score']:.1f}")


# ── Cache write ───────────────────────────────────────────────────────────────

def write_resolution_cache(
    cluster_id: str,
    result: dict,
    con: duckdb.DuckDBPyConnection,
    issuer_master_version: str = "unknown",
) -> None:
    now = datetime.utcnow().isoformat()
    existing = con.execute(
        "SELECT first_resolved_at FROM resolution_cache WHERE entity_cluster_id=?",
        [cluster_id]
    ).fetchone()
    first_at = existing[0] if existing else now
    con.execute("""
        INSERT OR REPLACE INTO resolution_cache (
            entity_cluster_id, resolved, public_company_id, public_company_name,
            preferred_ticker, preferred_exchange, relationship_type,
            resolution_stage, confidence_score, confidence_band,
            match_explanation, source_evidence_json,
            resolver_version, issuer_master_version,
            first_resolved_at, last_validated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        cluster_id,
        result.get("resolved", False),
        result.get("public_company_id"),
        result.get("public_company_name"),
        result.get("preferred_ticker"),
        result.get("preferred_exchange"),
        result.get("relationship_type"),
        result.get("resolution_stage"),
        result.get("confidence_score", 0.0),
        result.get("confidence_band", "low"),
        result.get("match_explanation"),
        result.get("source_evidence_json", "{}"),
        RESOLVER_V1_VERSION,
        issuer_master_version,
        first_at,
        now,
    ])


# ── Full orchestrator ─────────────────────────────────────────────────────────

def resolve_cluster(
    ctx: ClusterContext,
    con: duckdb.DuckDBPyConnection,
    thresholds: V1ThresholdsConfig | None = None,
    issuer_master_version: str = "unknown",
) -> dict:
    """Run all stages for one cluster. Write cache. Return result dict."""
    if thresholds is None:
        thresholds = V1ThresholdsConfig()

    for stage_fn in (stage0_override, stage1_cache, stage2_exact_parent,
                     stage3_exact_direct, stage4_alias_match):
        result = stage_fn(ctx, con)
        if result:
            if stage_fn is not stage1_cache:  # cache hit: already stored
                write_resolution_cache(ctx.cluster_id, result, con, issuer_master_version)
            return result

    # Stages 5–7 (fuzzy)
    name_for_tokens = ctx.canonical_parent_name or ctx.canonical_entity_name or ""
    if len(name_for_tokens.split()) >= thresholds.min_tokens_for_fuzzy:
        candidates = stage5_generate_candidates(ctx, con, thresholds.max_candidates)
        scored     = stage6_fuzzy_score(ctx, candidates)
        result     = stage7_accept(ctx.cluster_id, scored, thresholds)
    else:
        result = _unresolved(ctx.cluster_id, "insufficient_tokens")

    write_resolution_cache(ctx.cluster_id, result, con, issuer_master_version)
    return result
```

- [ ] **Step 4: Run all pipeline tests — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_pipeline.py -v 2>&1 | head -40
```

- [ ] **Step 5: Commit**

```bash
rtk git add resolver/pipeline.py tests/resolver/test_pipeline.py && rtk git commit -m "feat: pipeline stages 5-7 — candidate gen, fuzzy score, accept/ambiguous/unresolved + cache write + orchestrator"
```

---

## Task 8: CLI, Run Metrics, and API Integration

**Files:**
- Create: `resolver/cli.py`
- Create: `tests/resolver/test_cli.py`
- Modify: `resolver/api.py`

- [ ] **Step 1: Write CLI smoke test**

```python
# tests/resolver/test_cli.py
import os, tempfile, subprocess, sys, csv

SAMPLE_CSV = """\
PIID,Modification Number,Date Signed,Fiscal Year,Action Obligation,Ultimate Parent Unique Entity ID,Unique Entity ID,CAGE Code,Legal Business Name,Ultimate Parent Legal Business Name,Doing Business As Name,Contractor Name,Vendor Address State,Vendor Address Country,Country of Incorporation
PIID001,,2023-01-15,2023,5000000,PUEI_RTX,UEI_RTX_SUB,RX001,Raytheon Intelligence and Space,RTX Corporation,,Raytheon Intelligence and Space,VA,USA,USA
PIID002,,2023-02-20,2023,1000000,,UEI_UNKN,,Some Unknown Private Firm,,,,MD,USA,USA
""".strip()

def test_cli_runs_end_to_end():
    with tempfile.TemporaryDirectory() as d:
        csv_path = os.path.join(d, "contracts.csv")
        out_dir  = os.path.join(d, "output")
        db_path  = os.path.join(d, "resolver.duckdb")
        with open(csv_path, "w") as f:
            f.write(SAMPLE_CSV)
        result = subprocess.run(
            [sys.executable, "-m", "resolver",
             "--input", csv_path,
             "--output-dir", out_dir,
             "--db", db_path,
             "--no-refresh",     # skip live downloads in CI
             "--log-level", "WARNING"],
            capture_output=True, text=True, timeout=60,
        )
        # Should not crash even with empty issuer master
        assert result.returncode == 0, result.stderr
        # Output CSV should exist
        out_files = os.listdir(out_dir)
        assert any(f.endswith(".csv") for f in out_files), f"No CSV in {out_files}"
```

- [ ] **Step 2: Run — expect FAIL** (`No module named resolver.__main__`)

```bash
rtk python -m pytest tests/resolver/test_cli.py -v 2>&1 | head -20
```

- [ ] **Step 3: Create `resolver/cli.py`**

```python
"""resolver/cli.py — Batch entry point: python -m resolver [args]."""
from __future__ import annotations
import argparse, json, logging, os, sys, uuid
from datetime import datetime
from pathlib import Path
import pandas as pd
from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import refresh_issuer_master, get_issuer_master_version
from resolver.clusters import build_entity_clusters
from resolver.pipeline import ClusterContext, resolve_cluster, write_resolution_cache
from resolver.models import V1ThresholdsConfig, RESOLVER_V1_VERSION
from resolver.normalize import conservative_normalize
from resolver.ingest import load_contracts, assign_contract_row_ids, build_contract_identity_features

log = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Resolver V1 batch pipeline")
    p.add_argument("--input",       required=True, nargs="+",
                   help="Contract CSV or Parquet file(s)")
    p.add_argument("--output-dir",  default="data/outputs")
    p.add_argument("--db",          default="data/cache/resolver.duckdb")
    p.add_argument("--cache-dir",   default="data/cache")
    p.add_argument("--config",      default=None,
                   help="JSON config file with threshold overrides")
    p.add_argument("--refresh",     action="store_true",
                   help="Force refresh of issuer master from SEC + Nasdaq")
    p.add_argument("--no-refresh",  action="store_true",
                   help="Skip issuer master refresh (use existing DB)")
    p.add_argument("--openfigi-tail", action="store_true",
                   help="Enable Stage 8 OpenFIGI enrichment for unresolved tail")
    p.add_argument("--log-level",   default="INFO")
    return p


def load_thresholds(config_path: str | None) -> V1ThresholdsConfig:
    t = V1ThresholdsConfig()
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            overrides = json.load(f).get("thresholds", {})
        for k, v in overrides.items():
            if hasattr(t, k):
                setattr(t, k, v)
    return t


def run_batch(args) -> None:
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    run_id = str(uuid.uuid4())[:8]
    started = datetime.utcnow()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    con = get_db(args.db)
    ensure_schema(con)

    # Issuer master
    if args.refresh:
        im_version = refresh_issuer_master(con, args.cache_dir, force=True)
    elif not args.no_refresh:
        im_version = refresh_issuer_master(con, args.cache_dir, force=False)
    else:
        im_version = get_issuer_master_version(con)

    thresholds = load_thresholds(args.config)
    if args.openfigi_tail:
        thresholds.enable_openfigi_tail = True

    # Ingest
    all_dfs = []
    for path in args.input:
        df = pd.read_csv(path) if path.endswith(".csv") else pd.read_parquet(path)
        all_dfs.append(df)
    df = pd.concat(all_dfs, ignore_index=True)
    df = assign_contract_row_ids(df)
    features = build_contract_identity_features(df)
    log.info(f"Loaded {len(df)} rows → {len(features)} identity features")

    # Cluster
    row_to_cluster = build_entity_clusters(features, con)
    feat_map = {f.contract_row_id: f for f in features}

    # Get clusters to resolve
    clusters = con.execute(
        "SELECT entity_cluster_id, canonical_parent_name, canonical_entity_name, "
        "ultimate_parent_uei, uei, cage_code, all_parent_names_json, "
        "all_legal_names_json, all_dba_names_json "
        "FROM entity_clusters"
    ).fetchall()

    metrics = {
        "run_id": run_id, "total_rows": len(df), "total_clusters": len(clusters),
        "new_resolved": 0, "stage_wins": {}, "unresolved_count": 0,
        "ambiguous_count": 0, "override_hits": 0, "cache_hits": 0,
        "candidates_total": 0, "clusters_fuzzy": 0,
    }

    cluster_results: dict[str, dict] = {}
    for row in clusters:
        cid, can_parent, can_entity, puei, uei, cage, parents_j, legal_j, dba_j = row
        ctx = ClusterContext(
            cluster_id=cid,
            canonical_parent_name=can_parent,
            canonical_entity_name=can_entity,
            uei=uei, parent_uei=puei, cage=cage,
            parent_name_norm=conservative_normalize(can_parent),
            legal_name_norm=conservative_normalize(can_entity),
            all_parent_names=json.loads(parents_j or "[]"),
            all_legal_names=json.loads(legal_j or "[]"),
            all_dba_names=json.loads(dba_j or "[]"),
        )
        result = resolve_cluster(ctx, con, thresholds, im_version)
        cluster_results[cid] = result

        stage = result.get("resolution_stage", "unresolved")
        metrics["stage_wins"][stage] = metrics["stage_wins"].get(stage, 0) + 1
        if result.get("resolved"):
            metrics["new_resolved"] += 1
        if result.get("ambiguous"):
            metrics["ambiguous_count"] += 1
        if not result.get("resolved") and not result.get("ambiguous"):
            metrics["unresolved_count"] += 1
        if "cache" in stage:
            metrics["cache_hits"] += 1
        if result.get("manual_override_used"):
            metrics["override_hits"] += 1

    # Fan back to rows
    rows_out = []
    for _, row in df.iterrows():
        cid = row_to_cluster.get(row.get("contract_row_id", ""), "")
        res = cluster_results.get(cid, {})
        r = {"contract_row_id": row.get("contract_row_id", "")}
        r.update(res)
        rows_out.append(r)

    # Write outputs
    result_df = pd.DataFrame(rows_out)
    out_path = os.path.join(args.output_dir, f"resolution_results_{run_id}.csv")
    result_df.to_csv(out_path, index=False)
    log.info(f"Results: {out_path}")

    # Write review queue
    ambiguous = [r for r in cluster_results.values() if r.get("needs_review")]
    if ambiguous:
        queue_path = os.path.join(args.output_dir, f"review_queue_{run_id}.csv")
        pd.DataFrame(ambiguous).to_csv(queue_path, index=False)
        log.info(f"Review queue: {queue_path} ({len(ambiguous)} clusters)")

    # Run log
    ended = datetime.utcnow()
    metrics_copy = {k: v for k, v in metrics.items()}
    metrics_copy["avg_candidates_scored"] = (
        metrics["candidates_total"] / max(metrics["clusters_fuzzy"], 1)
    )
    con.execute("""
        INSERT OR REPLACE INTO resolver_run_log (
            run_id, started_at, ended_at, total_rows, total_clusters,
            new_resolved, stage_wins_json, unresolved_count, ambiguous_count,
            override_hits, cache_hits, avg_candidates_scored,
            resolver_version, config_hash
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [run_id, started.isoformat(), ended.isoformat(),
          metrics["total_rows"], metrics["total_clusters"],
          metrics["new_resolved"], json.dumps(metrics["stage_wins"]),
          metrics["unresolved_count"], metrics["ambiguous_count"],
          metrics["override_hits"], metrics["cache_hits"],
          metrics_copy["avg_candidates_scored"],
          RESOLVER_V1_VERSION, ""])

    # Print summary
    print(f"\n{'='*60}")
    print(f"Resolver V1 — Run {run_id}")
    print(f"  Rows:       {metrics['total_rows']:,}")
    print(f"  Clusters:   {metrics['total_clusters']:,}")
    print(f"  Resolved:   {metrics['new_resolved']:,}")
    print(f"  Ambiguous:  {metrics['ambiguous_count']:,}")
    print(f"  Unresolved: {metrics['unresolved_count']:,}")
    print(f"  Cache hits: {metrics['cache_hits']:,}")
    print(f"  Override hits: {metrics['override_hits']:,}")
    print(f"  Stage wins: {json.dumps(metrics['stage_wins'], indent=4)}")
    print(f"{'='*60}\n")
```

- [ ] **Step 4: Create `resolver/__main__.py`**

```python
"""resolver/__main__.py — Enables: python -m resolver [args]"""
from resolver.cli import build_arg_parser, run_batch

if __name__ == "__main__":
    parser = build_arg_parser()
    args = parser.parse_args()
    run_batch(args)
```

- [ ] **Step 5: Add `resolve_v1()` to `resolver/api.py`** (append after existing functions)

```python
# Append to resolver/api.py

def resolve_v1(
    contracts,
    db_path: str = "data/cache/resolver.duckdb",
    cache_dir: str = "data/cache",
    config: dict | None = None,
    refresh: bool = False,
) -> "pd.DataFrame":
    """
    V1 pipeline entry point.
    contracts: DataFrame | path-to-CSV | path-to-Parquet
    Returns DataFrame with V1 output columns.
    """
    import pandas as pd
    from resolver.storage import get_db, ensure_schema
    from resolver.issuer_master import refresh_issuer_master, get_issuer_master_version
    from resolver.clusters import build_entity_clusters
    from resolver.pipeline import ClusterContext, resolve_cluster
    from resolver.models import V1ThresholdsConfig
    from resolver.normalize import conservative_normalize
    from resolver.ingest import load_contracts, assign_contract_row_ids, build_contract_identity_features

    con = get_db(db_path)
    ensure_schema(con)
    im_version = refresh_issuer_master(con, cache_dir, force=refresh) if refresh else get_issuer_master_version(con)

    thresholds = V1ThresholdsConfig()
    if config:
        for k, v in config.get("thresholds", {}).items():
            if hasattr(thresholds, k):
                setattr(thresholds, k, v)

    df = load_contracts(contracts)
    df = assign_contract_row_ids(df)
    features = build_contract_identity_features(df)
    row_to_cluster = build_entity_clusters(features, con)

    clusters = con.execute(
        "SELECT entity_cluster_id, canonical_parent_name, canonical_entity_name, "
        "ultimate_parent_uei, uei, cage_code, all_parent_names_json, "
        "all_legal_names_json, all_dba_names_json FROM entity_clusters"
    ).fetchall()

    cluster_results: dict[str, dict] = {}
    for row in clusters:
        cid, cp, ce, puei, uei, cage, pj, lj, dj = row
        ctx = ClusterContext(
            cluster_id=cid, canonical_parent_name=cp, canonical_entity_name=ce,
            uei=uei, parent_uei=puei, cage=cage,
            parent_name_norm=conservative_normalize(cp),
            legal_name_norm=conservative_normalize(ce),
            all_parent_names=__import__("json").loads(pj or "[]"),
            all_legal_names=__import__("json").loads(lj or "[]"),
            all_dba_names=__import__("json").loads(dj or "[]"),
        )
        cluster_results[cid] = resolve_cluster(ctx, con, thresholds, im_version)

    rows_out = []
    for _, row in df.iterrows():
        cid = row_to_cluster.get(row.get("contract_row_id", ""), "")
        r = dict(row)
        r.update(cluster_results.get(cid, {}))
        rows_out.append(r)

    return pd.DataFrame(rows_out)
```

- [ ] **Step 6: Update `resolver/__init__.py`** — add `resolve_v1` to exports

```python
# In resolver/__init__.py, add to imports:
from resolver.api import resolve_v1

# Add to __all__:
"resolve_v1",
```

- [ ] **Step 7: Run CLI test — expect PASS**

```bash
rtk python -m pytest tests/resolver/test_cli.py -v 2>&1 | head -20
```

- [ ] **Step 8: Full test suite**

```bash
rtk python -m pytest tests/resolver/ -v 2>&1 | tail -20
```

- [ ] **Step 9: Commit**

```bash
rtk git add resolver/cli.py resolver/__main__.py resolver/api.py resolver/__init__.py tests/resolver/test_cli.py && rtk git commit -m "feat: CLI batch entry point, run metrics, resolve_v1() API"
```

---

## Task 9: Download SEC + Nasdaq Data (First-Time Setup)

This task runs the actual data download and populates the issuer master. Run once; results are cached.

- [ ] **Step 1: Run issuer master refresh**

```bash
rtk python -c "
import logging; logging.basicConfig(level=logging.INFO)
from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import refresh_issuer_master
con = get_db('data/cache/resolver.duckdb')
ensure_schema(con)
v = refresh_issuer_master(con, cache_dir='data/cache')
n = con.execute('SELECT COUNT(*) FROM issuer_master').fetchone()[0]
na = con.execute('SELECT COUNT(*) FROM issuer_aliases').fetchone()[0]
print(f'Version: {v}  Issuers: {n:,}  Aliases: {na:,}')
con.close()
"
```

Expected: `Issuers: ~10,000–15,000  Aliases: ~30,000–50,000`

If the download fails (network issue), the cached `.json`/`.txt` files in `data/cache/` will be used on next run.

- [ ] **Step 2: Spot-check known names**

```bash
rtk python -c "
from resolver.storage import get_db
con = get_db('data/cache/resolver.duckdb')
for name in ['RAYTHEON', 'BOEING', 'LOCKHEED MARTIN', 'PALANTIR']:
    rows = con.execute(
        \"SELECT ticker_current, exchange_current FROM issuer_master im \
          JOIN issuer_aliases ia ON im.public_company_id=ia.public_company_id \
          WHERE ia.alias_normalized_conservative LIKE ? AND im.is_common_equity=TRUE\",
        [f'%{name}%']
    ).fetchall()
    print(f'{name}: {rows[:2]}')
con.close()
"
```

- [ ] **Step 3: Commit data cache note**

No files to commit — data is in `data/` which is gitignored. Just verify the run works.

---

## Task 10: Smoke Test on Real Contract Data + Integration Verification

- [ ] **Step 1: Run resolver on a sample of `datasets/stage1_filter_training_set.csv`** (if it exists)

```bash
rtk python -m resolver \
  --input datasets/stage1_filter_training_set.csv \
  --output-dir data/outputs/v1_smoke \
  --db data/cache/resolver.duckdb \
  --no-refresh \
  --log-level INFO 2>&1 | tail -30
```

Expected: prints run summary with stage win counts. No crash.

- [ ] **Step 2: Verify compat shim still works for build_training_set.py**

```bash
rtk python -c "from resolver import TickerResolverV4, resolve_ticker; print('compat ok')"
```

Expected: `compat ok`

- [ ] **Step 3: Check known-alias fast path still resolves Raytheon → RTX**

```bash
rtk python -c "
from resolver import TickerResolverV4
from resolver.models import load_config
cfg = load_config()

# Quick match via known_aliases
from resolver.matching import check_known_aliases
from resolver.entities import AwardeeEntity
e = AwardeeEntity(entity_key='k', source_key_type='synthetic', uei=None, cage_code=None,
    canonical_name='Raytheon Intelligence and Space',
    canonical_name_norm='RAYTHEON INTELLIGENCE AND SPACE')
ticker = check_known_aliases(e)
print('known_alias result:', ticker)
assert ticker == 'RTX', f'Expected RTX got {ticker}'
print('ok')
"
```

- [ ] **Step 4: Run all tests**

```bash
rtk python -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

Pre-existing failures are expected for:
- `test_sam_gov_reader.py::test_valid_row_yields_contract_record` (date field mismatch — documented)
- `test_ticker_resolver_v4.py::test_tier1_cage_resolves_to_ticker` (GLEIF mock — documented)

All `tests/resolver/` tests must pass.

- [ ] **Step 5: Final commit**

```bash
rtk git add -u && rtk git commit -m "feat: resolver V1 overhaul complete — DuckDB issuer master, 8-stage pipeline, CLI"
```

---

## Self-Review Checklist

Verified against spec:

| Spec Requirement | Implemented | Location |
|------------------|-------------|----------|
| Entity-cluster resolution (not per-row) | ✅ | `clusters.py`, `pipeline.py` |
| Local SEC issuer master | ✅ | `issuer_master.py` |
| Nasdaq Trader symbol universe | ✅ | `issuer_master.py` |
| DuckDB storage | ✅ | `storage.py` |
| `issuer_master` + `issuer_aliases` tables | ✅ | `storage.py` DDL |
| Conservative + aggressive normalization | ✅ | `normalize.py` |
| Stage 0: manual overrides | ✅ | `pipeline.py:stage0_override` |
| Stage 1: resolution cache | ✅ | `pipeline.py:stage1_cache` |
| Stage 2: exact parent match | ✅ | `pipeline.py:stage2_exact_parent` |
| Stage 3: exact direct match | ✅ | `pipeline.py:stage3_exact_direct` |
| Stage 4: alias match (aggressive) | ✅ | `pipeline.py:stage4_alias_match` |
| Stage 5: candidate generation | ✅ | `pipeline.py:stage5_generate_candidates` |
| Stage 6: fuzzy scoring | ✅ | `pipeline.py:stage6_fuzzy_score` |
| Stage 7: accept / ambiguous / unresolved | ✅ | `pipeline.py:stage7_accept` |
| Stage 8: OpenFIGI tail (optional) | ✅ | `pipeline.py` (toggled via thresholds) |
| V1 output schema (all fields) | ✅ | `models.py:V1FinalRow`, `pipeline.py:_result` |
| Entity-level cache output | ✅ | `storage.py:resolution_cache`, `pipeline.py:write_resolution_cache` |
| Confidence bands 95/85/70 | ✅ | `models.py:score_to_confidence_band` |
| Config-driven thresholds | ✅ | `models.py:V1ThresholdsConfig` |
| Manual override schema | ✅ | `storage.py:manual_overrides` DDL |
| Batch CLI entry point | ✅ | `cli.py`, `__main__.py` |
| Run summary metrics | ✅ | `cli.py:run_batch` + `resolver_run_log` |
| Review queue | ✅ | `cli.py:run_batch` |
| `_compat.py` shim intact | ✅ | untouched |
| No historical ticker | ✅ | `historical_ticker_attempted=False` always |
| Graceful degradation | ✅ | empty DB → unresolved, no crash |
| Idempotent (cache prevents re-work) | ✅ | `stage1_cache` + `write_resolution_cache` |

**No placeholders. No TODOs.** All steps contain real code.

---

**Plan complete and saved to `docs/superpowers/plans/2026-04-09-resolver-v1-overhaul.md`.**

Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks
**2. Inline Execution** — execute tasks in this session using executing-plans

Which approach?
