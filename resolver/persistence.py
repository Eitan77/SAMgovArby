"""resolver/persistence.py — SQLite cache, Parquet I/O, DuckDB, manifests."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ── Directory helpers ─────────────────────────────────────────────────────────

def ensure_dirs(config) -> None:
    from resolver.models import PathsConfig
    p = config.paths
    for d in [p.raw_dir, p.staging_dir, p.curated_dir, p.index_dir,
              p.cache_dir, p.output_dir, p.manifest_dir]:
        Path(d).mkdir(parents=True, exist_ok=True)

# ── SQLite cache ──────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS http_cache (
    cache_key   TEXT PRIMARY KEY,
    url         TEXT,
    response    TEXT,
    cached_at   TEXT,
    expires_at  TEXT
);
CREATE TABLE IF NOT EXISTS openfigi_cache (
    cache_key   TEXT PRIMARY KEY,
    request     TEXT,
    response    TEXT,
    cached_at   TEXT
);
CREATE TABLE IF NOT EXISTS resolution_cache (
    entity_key        TEXT,
    award_date_bucket TEXT,
    reference_version TEXT,
    config_hash       TEXT,
    payload           TEXT,
    cached_at         TEXT,
    PRIMARY KEY (entity_key, award_date_bucket, reference_version, config_hash)
);
CREATE TABLE IF NOT EXISTS refresh_manifest (
    source_name  TEXT,
    started_at   TEXT,
    ended_at     TEXT,
    status       TEXT,
    rows_written INTEGER,
    details      TEXT,
    error        TEXT
);
CREATE TABLE IF NOT EXISTS overrides (
    entity_key      TEXT PRIMARY KEY,
    fixed_issuer    TEXT,
    fixed_ticker    TEXT,
    forced_null     INTEGER DEFAULT 0,
    award_date_from TEXT,
    award_date_to   TEXT,
    reason          TEXT,
    reviewer        TEXT,
    created_at      TEXT
);
CREATE TABLE IF NOT EXISTS audit_index (
    resolution_id TEXT PRIMARY KEY,
    entity_key    TEXT,
    award_date    TEXT,
    status        TEXT,
    ticker        TEXT,
    confidence    TEXT,
    created_at    TEXT
);
"""

def _get_db(sqlite_path: str) -> sqlite3.Connection:
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_sqlite_cache(config) -> None:
    with _get_db(config.paths.sqlite_path) as conn:
        conn.executescript(_SCHEMA)

def get_cached_http_response(cache_key: str, config) -> dict | None:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            row = conn.execute(
                "SELECT response, expires_at FROM http_cache WHERE cache_key=?",
                (cache_key,)
            ).fetchone()
            if not row:
                return None
            if row["expires_at"]:
                if datetime.fromisoformat(row["expires_at"]) < datetime.utcnow():
                    return None
            return json.loads(row["response"])
    except Exception as e:
        log.debug(f"HTTP cache get failed: {e}")
        return None

def put_cached_http_response(cache_key: str, response: Any, metadata: dict, config) -> None:
    try:
        expires_at = metadata.get("expires_at")
        with _get_db(config.paths.sqlite_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO http_cache (cache_key, url, response, cached_at, expires_at)"
                " VALUES (?,?,?,?,?)",
                (cache_key, metadata.get("url",""), json.dumps(response),
                 datetime.utcnow().isoformat(), expires_at)
            )
    except Exception as e:
        log.debug(f"HTTP cache put failed: {e}")

def get_cached_resolution(entity_key: str, award_date_bucket: str, config, reference_version: str = "unknown") -> dict | None:
    try:
        cfg_hash = config.config_hash()
        with _get_db(config.paths.sqlite_path) as conn:
            row = conn.execute(
                "SELECT payload FROM resolution_cache"
                " WHERE entity_key=? AND award_date_bucket=? AND reference_version=? AND config_hash=?",
                (entity_key, award_date_bucket, reference_version, cfg_hash)
            ).fetchone()
            if row:
                return json.loads(row["payload"])
    except Exception as e:
        log.debug(f"Resolution cache get failed: {e}")
    return None

def put_cached_resolution(entity_key: str, award_date_bucket: str, payload: dict, config, reference_version: str = "unknown") -> None:
    try:
        cfg_hash = config.config_hash()
        with _get_db(config.paths.sqlite_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO resolution_cache"
                " (entity_key, award_date_bucket, reference_version, config_hash, payload, cached_at)"
                " VALUES (?,?,?,?,?,?)",
                (entity_key, award_date_bucket, reference_version, cfg_hash,
                 json.dumps(payload), datetime.utcnow().isoformat())
            )
    except Exception as e:
        log.debug(f"Resolution cache put failed: {e}")

def load_overrides_from_db(config) -> list[dict]:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            rows = conn.execute("SELECT * FROM overrides").fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.debug(f"Load overrides failed: {e}")
        return []

def save_override_to_db(override: dict, config) -> None:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO overrides"
                " (entity_key, fixed_issuer, fixed_ticker, forced_null,"
                "  award_date_from, award_date_to, reason, reviewer, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    override.get("entity_key"),
                    override.get("fixed_issuer"),
                    override.get("fixed_ticker"),
                    int(override.get("forced_null", False)),
                    override.get("award_date_from"),
                    override.get("award_date_to"),
                    override.get("reason"),
                    override.get("reviewer"),
                    override.get("created_at", datetime.utcnow().isoformat()),
                )
            )
    except Exception as e:
        log.warning(f"Save override failed: {e}")

def record_refresh_event(source_name: str, started_at: datetime, ended_at: datetime,
                          status: str, details: dict, config, error: str | None = None,
                          rows_written: int = 0) -> None:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            conn.execute(
                "INSERT INTO refresh_manifest"
                " (source_name, started_at, ended_at, status, rows_written, details, error)"
                " VALUES (?,?,?,?,?,?,?)",
                (source_name, started_at.isoformat(), ended_at.isoformat(),
                 status, rows_written, json.dumps(details), error)
            )
    except Exception as e:
        log.warning(f"Record refresh event failed: {e}")

def get_latest_refresh_status(source_name: str, config) -> dict | None:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            row = conn.execute(
                "SELECT * FROM refresh_manifest WHERE source_name=?"
                " ORDER BY ended_at DESC LIMIT 1",
                (source_name,)
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None

def list_available_reference_versions(config) -> list[dict]:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            rows = conn.execute(
                "SELECT source_name, MAX(ended_at) as latest_at, status, rows_written"
                " FROM refresh_manifest WHERE status='ok'"
                " GROUP BY source_name"
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []

def write_audit_record(resolution_id: str, entity_key: str | None, award_date: str | None,
                        status: str, ticker: str | None, confidence: str, config) -> None:
    try:
        with _get_db(config.paths.sqlite_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO audit_index"
                " (resolution_id, entity_key, award_date, status, ticker, confidence, created_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (resolution_id, entity_key, award_date, status, ticker,
                 confidence, datetime.utcnow().isoformat())
            )
    except Exception as e:
        log.debug(f"Audit write failed: {e}")

# ── Parquet I/O ───────────────────────────────────────────────────────────────

def read_parquet(path: str):
    import pyarrow.parquet as pq
    return pq.read_table(path).to_pandas()

def write_parquet(df, path: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if hasattr(df, "to_pandas"):
        # polars
        df = df.to_pandas()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression="snappy")

def write_partitioned_parquet(df, path: str, partition_cols: list[str]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if hasattr(df, "to_pandas"):
        df = df.to_pandas()
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=path, partition_cols=partition_cols, compression="snappy")

# ── DuckDB helpers ────────────────────────────────────────────────────────────

def get_duckdb_connection(config):
    try:
        import duckdb
        Path(config.paths.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(config.paths.duckdb_path)
    except ImportError:
        raise ImportError("duckdb is required: pip install duckdb")

def run_query(conn, sql: str, params: list | None = None):
    if params:
        return conn.execute(sql, params).fetchdf()
    return conn.execute(sql).fetchdf()

def register_table(conn, name: str, df) -> None:
    import pandas as pd
    if not isinstance(df, pd.DataFrame):
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()
    conn.register(name, df)

# ── Manifests ─────────────────────────────────────────────────────────────────

def build_snapshot_manifest(source_name: str, files: list[str], metadata: dict) -> dict:
    import hashlib
    content = source_name + str(sorted(files)) + str(metadata)
    snap_id = hashlib.md5(content.encode()).hexdigest()[:12]
    return {
        "snapshot_id": snap_id,
        "source_name": source_name,
        "files": files,
        "metadata": metadata,
        "created_at": datetime.utcnow().isoformat(),
    }

def get_reference_snapshot_id(source_name: str) -> str:
    return f"{source_name}_{datetime.utcnow().strftime('%Y%m%d')}"
