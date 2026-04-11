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

_DDL_STATEMENTS = [
"""CREATE TABLE IF NOT EXISTS contracts_raw (
    contract_row_id TEXT PRIMARY KEY,
    source_file     TEXT,
    ingested_at     TEXT,
    raw_json        TEXT
)""",
"""CREATE TABLE IF NOT EXISTS contracts_normalized (
    contract_row_id               TEXT PRIMARY KEY,
    entity_cluster_id             TEXT,
    ultimate_parent_uei           TEXT,
    uei                           TEXT,
    cage_code                     TEXT,
    legal_business_name_raw       TEXT,
    legal_business_name_norm_cons TEXT,
    legal_business_name_norm_agg  TEXT,
    parent_name_raw               TEXT,
    parent_name_norm_cons         TEXT,
    parent_name_norm_agg          TEXT,
    dba_name_raw                  TEXT,
    contractor_name_raw           TEXT,
    vendor_state                  TEXT,
    vendor_country                TEXT,
    country_of_incorporation      TEXT,
    date_signed                   TEXT,
    fiscal_year                   INTEGER,
    piid                          TEXT,
    dollars_obligated             DOUBLE
)""",
"""CREATE TABLE IF NOT EXISTS entity_edges (
    edge_id           TEXT PRIMARY KEY,
    entity_cluster_id TEXT NOT NULL,
    contract_row_id   TEXT NOT NULL,
    edge_type         TEXT
)""",
"""CREATE TABLE IF NOT EXISTS entity_clusters (
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
)""",
"""CREATE TABLE IF NOT EXISTS issuer_master (
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
)""",
"""CREATE TABLE IF NOT EXISTS issuer_aliases (
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
)""",
"""CREATE TABLE IF NOT EXISTS resolution_cache (
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
)""",
"""CREATE TABLE IF NOT EXISTS manual_overrides (
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
)""",
"""CREATE TABLE IF NOT EXISTS resolution_results (
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
)""",
"""CREATE TABLE IF NOT EXISTS review_queue (
    queue_id           TEXT PRIMARY KEY,
    entity_cluster_id  TEXT NOT NULL,
    run_id             TEXT NOT NULL,
    review_reason      TEXT,
    top_candidate_json TEXT,
    queued_at          TEXT,
    UNIQUE (entity_cluster_id, run_id)
)""",
"""CREATE TABLE IF NOT EXISTS resolver_run_log (
    run_id                TEXT PRIMARY KEY,
    started_at            TEXT,
    ended_at              TEXT,
    total_rows            INTEGER,
    total_clusters        INTEGER,
    new_resolved          INTEGER,
    stage_wins_json       TEXT,
    unresolved_count      INTEGER,
    ambiguous_count       INTEGER,
    override_hits         INTEGER,
    cache_hits            INTEGER,
    avg_candidates_scored DOUBLE,
    resolver_version      TEXT,
    config_hash           TEXT
)""",
"""CREATE TABLE IF NOT EXISTS openfigi_cache (
    cache_key TEXT PRIMARY KEY,
    request   TEXT,
    response  TEXT,
    cached_at TEXT
)""",
]

_connections: dict[str, duckdb.DuckDBPyConnection] = {}


def get_db(path: str = "data/cache/resolver.duckdb") -> duckdb.DuckDBPyConnection:
    if path not in _connections:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        _connections[path] = duckdb.connect(path)
    return _connections[path]


_INDEX_STATEMENTS = [
    # issuer_aliases — exact and prefix lookups in stages 2/3/4
    "CREATE INDEX IF NOT EXISTS idx_alias_cons ON issuer_aliases(alias_normalized_conservative)",
    "CREATE INDEX IF NOT EXISTS idx_alias_agg  ON issuer_aliases(alias_normalized_aggressive)",
    "CREATE INDEX IF NOT EXISTS idx_alias_pub  ON issuer_aliases(public_company_id)",
    # issuer_master — join target + active/equity filters
    "CREATE INDEX IF NOT EXISTS idx_master_pub    ON issuer_master(public_company_id)",
    "CREATE INDEX IF NOT EXISTS idx_master_ticker ON issuer_master(ticker_current)",
    # resolution_cache — primary key lookup, already fast via PK but explicit helps
    "CREATE INDEX IF NOT EXISTS idx_cache_cluster ON resolution_cache(entity_cluster_id)",
    # manual_overrides — stage0 lookups by key_type + key_value
    "CREATE INDEX IF NOT EXISTS idx_override_key ON manual_overrides(override_key_type, override_key_value)",
    # entity_clusters — outer loop
    "CREATE INDEX IF NOT EXISTS idx_cluster_uei  ON entity_clusters(uei)",
    "CREATE INDEX IF NOT EXISTS idx_cluster_cage ON entity_clusters(cage_code)",
]


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in _DDL_STATEMENTS:
        con.execute(stmt)
    for stmt in _INDEX_STATEMENTS:
        con.execute(stmt)
    log.debug("DuckDB schema ensured.")


def close_all() -> None:
    for con in _connections.values():
        try:
            con.close()
        except Exception:
            pass
    _connections.clear()
