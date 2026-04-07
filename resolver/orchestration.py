"""resolver/orchestration.py — Refresh, entity resolution, contract resolution, export."""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, date
from typing import Any

import pandas as pd

from resolver.models import (
    FinalResolution, ResolverConfig, ReferenceHandles,
    ResolverStatus, ContractIdentityFeatures,
)
from resolver.normalize import historical_symbol_coverage_bucket
from resolver.persistence import (
    ensure_dirs, init_sqlite_cache, get_cached_resolution, put_cached_resolution,
    write_audit_record,
)
from resolver.ingest import (
    load_contracts, assign_contract_row_ids, build_contract_identity_features,
    refresh_sec_submissions, refresh_gleif_data, refresh_sam_optional_extract,
)
from resolver.reference import build_reference_handles, build_all_reference_tables, refresh_isin_lei_data
from resolver.entities import build_entity_graph, fan_entity_keys_to_contract_rows, build_entity_resolution_inputs
from resolver.matching import resolve_parent_first, merge_entity_and_historical_decisions
from resolver.historical import resolve_historical_ticker, select_us_tradable_security, get_market_cap
from resolver.overrides import load_overrides, apply_overrides_to_entity

log = logging.getLogger(__name__)

# ── Refresh orchestration ─────────────────────────────────────────────────────

def refresh_all_sources(config: ResolverConfig, force: bool = False):
    ensure_dirs(config)
    init_sqlite_cache(config)
    sec   = refresh_sec(config, force)
    gleif = refresh_gleif(config, force)
    isin  = refresh_isin_lei(config, force)
    sam   = refresh_sam(config, force)
    rebuild_reference_masters(config)
    return [sec, gleif, isin, sam]

def refresh_sec(config: ResolverConfig, force: bool = False):
    return refresh_sec_submissions(config)

def refresh_gleif(config: ResolverConfig, force: bool = False):
    return refresh_gleif_data(config)

def refresh_isin_lei(config: ResolverConfig, force: bool = False):
    return refresh_isin_lei_data(config)

def refresh_sam(config: ResolverConfig, force: bool = False):
    return refresh_sam_optional_extract(config)

def rebuild_reference_masters(config: ResolverConfig):
    return build_all_reference_tables(config)

# ── Entity resolution orchestration ──────────────────────────────────────────

def prepare_entity_inputs(
    features:  list[ContractIdentityFeatures],
    config:    ResolverConfig,
) -> dict:
    graph = build_entity_graph(features, config)
    inputs = build_entity_resolution_inputs(graph["awardees"], graph["parents"])
    return {"graph": graph, "inputs": inputs}

def resolve_all_entities(
    entity_inputs: dict,
    references:    ReferenceHandles,
    config:        ResolverConfig,
    overrides:     dict | None = None,
) -> dict[str, Any]:
    """Resolve every entity. Returns dict[entity_key → resolution_metadata]."""
    graph    = entity_inputs["graph"]
    inputs   = entity_inputs["inputs"]
    overrides = overrides or {}
    decisions: dict[str, Any] = {}

    for inp in inputs:
        entity_key      = inp["entity_key"]
        awardee         = inp["awardee"]
        parent          = inp["parent"]
        parent_key      = inp["parent_entity_key"]

        award_date  = awardee.first_seen_date  # use first seen as representative
        bucket      = historical_symbol_coverage_bucket(award_date) or "unknown"

        # Check overrides
        override_decision = apply_overrides_to_entity(entity_key, award_date, overrides)
        if override_decision:
            meta = _build_meta_from_override(override_decision, awardee, config, references)
            meta["resolver_manual_override_applied"] = True
            decisions[entity_key] = meta
            put_cached_resolution(entity_key, bucket, meta, config, references.reference_version)
            continue

        # Resolve parent-first
        entity_decision = resolve_parent_first(awardee, parent, references, config)

        # Historical symbol
        cik             = entity_decision.matched_cik
        lei             = entity_decision.matched_lei
        known_alias     = entity_decision.evidence_json.get("ticker") if entity_decision.resolution_path == "known_alias" else None
        hist_decision   = resolve_historical_ticker(cik, lei, award_date, references, config, known_alias)

        # Security selection
        sec_decision = select_us_tradable_security(entity_decision, hist_decision, references, config)

        # Merge
        meta = merge_entity_and_historical_decisions(entity_decision, hist_decision, sec_decision, config)
        meta["resolver_entity_key"]        = entity_key
        meta["resolver_parent_entity_key"] = parent_key
        meta["resolver_awardee_name_norm"]  = awardee.canonical_name_norm
        meta["resolver_parent_name_norm"]   = parent.canonical_name_norm if parent else None
        meta["resolver_award_date_used"]    = str(award_date) if award_date else None

        # Market cap
        ticker = meta.get("resolver_ticker")
        if ticker:
            mcap = get_market_cap(ticker, references.security_cache)
            meta["market_cap_current"] = mcap

        # Resolution ID
        resolution_id = hashlib.md5(
            f"{entity_key}:{bucket}:{ticker}".encode()
        ).hexdigest()[:16]
        meta["resolver_resolution_id"] = resolution_id
        meta["resolver_version"]        = "2.0"

        # Confidence from score
        top_score = meta.get("resolver_top_candidate_score") or 0.0
        meta.setdefault("resolver_confidence", _score_to_confidence(top_score))

        decisions[entity_key] = meta

    return decisions

def _build_meta_from_override(override_decision, awardee, config, references) -> dict:
    """Build a full metadata dict from a manual override decision."""
    from resolver.models import ResolverStatus
    from resolver.historical import HistoricalSymbolDecision, SecuritySelectionDecision
    ticker = override_decision.evidence_json.get("fixed_ticker")
    if override_decision.decision_status == "no_match":
        status = ResolverStatus.NULL_PRIVATE.value
        ticker = None
    else:
        status = ResolverStatus.RESOLVED.value
    return {
        "resolver_status":    status,
        "resolver_ticker":    ticker,
        "resolver_exchange":  None,
        "resolver_security_type": None,
        "resolver_is_adr":    None,
        "resolver_confidence": "high" if ticker else "none",
        "resolver_resolution_path": "manual_override",
        "resolver_null_reason": "override_forced_null" if not ticker else None,
        "resolver_matched_cik": None,
        "resolver_matched_lei": None,
        "resolver_matched_issuer_name": override_decision.matched_issuer_name,
        "resolver_top_candidate_score": 100.0,
        "resolver_second_candidate_score": None,
        "resolver_candidate_gap": None,
        "resolver_score_breakdown_json": json.dumps(override_decision.evidence_json),
    }

def persist_entity_decisions(entity_decisions: dict, config: ResolverConfig) -> None:
    """Write entity decisions to Parquet output."""
    from resolver.persistence import write_parquet
    import os
    if not entity_decisions:
        return
    rows = list(entity_decisions.values())
    df   = pd.DataFrame(rows)
    path = os.path.join(config.paths.output_dir, "entity_decisions.parquet")
    write_parquet(df, path)
    log.info(f"Entity decisions written: {len(rows):,} rows → {path}")

# ── Contract resolution orchestration ────────────────────────────────────────

def attach_entity_decisions_to_contracts(
    contract_key_map: list[dict],
    entity_decisions: dict,
) -> list[dict]:
    """Fan entity decisions back to contract rows."""
    rows = []
    for item in contract_key_map:
        entity_key = item.get("entity_key")
        decision   = entity_decisions.get(entity_key, {})
        row        = dict(item)
        row.update(decision)
        rows.append(row)
    return rows

def resolve_contract_tickers(
    df:                 pd.DataFrame,
    references:         ReferenceHandles,
    config:             ResolverConfig,
    overrides:          dict | None = None,
    include_diagnostics: bool = True,
) -> pd.DataFrame:
    """
    Full pipeline:
    1. canonicalize → 2. identity features → 3. entity graph →
    4. resolve entities → 5. fan back → 6. attach diagnostics
    """
    # Assign row IDs
    df = assign_contract_row_ids(df)

    # Build identity features
    features = build_contract_identity_features(df)

    # Entity graph
    entity_inputs = prepare_entity_inputs(features, config)

    # Load overrides
    if overrides is None:
        try:
            overrides = load_overrides(config)
        except Exception:
            overrides = {}

    # Resolve
    entity_decisions = resolve_all_entities(entity_inputs, references, config, overrides)

    # Fan back to contract rows
    contract_key_map = fan_entity_keys_to_contract_rows(
        features,
        entity_inputs["graph"]["awardees"],
        entity_inputs["graph"]["parents"],
    )
    resolved_rows = attach_entity_decisions_to_contracts(contract_key_map, entity_decisions)

    # Merge onto original df
    resolver_df = pd.DataFrame(resolved_rows)
    if "contract_row_id" in resolver_df.columns and "contract_row_id" in df.columns:
        result = df.merge(
            resolver_df.drop(columns=["entity_key", "parent_entity_key"], errors="ignore"),
            on="contract_row_id",
            how="left",
        )
    else:
        result = df.copy()
        for col, vals in resolver_df.items():
            if col != "contract_row_id":
                result[col] = vals.values[:len(result)] if len(vals) <= len(result) else vals.values[:len(result)]

    result = apply_final_status_rules(result, config)
    return result

def apply_final_status_rules(df: pd.DataFrame, config: ResolverConfig) -> pd.DataFrame:
    """Post-process: fill missing status, add resolver_version."""
    df = df.copy()
    if "resolver_status" not in df.columns:
        df["resolver_status"] = ResolverStatus.NULL_ERROR.value
    else:
        df["resolver_status"] = df["resolver_status"].fillna(ResolverStatus.NULL_ERROR.value)
    df["resolver_version"] = "2.0"
    return df

# ── Export ────────────────────────────────────────────────────────────────────

def build_output_dataframe(raw_contracts: pd.DataFrame, resolver_results: pd.DataFrame) -> pd.DataFrame:
    """Merge raw contracts with resolver result columns."""
    resolver_cols = [c for c in resolver_results.columns if c.startswith("resolver_") or c == "market_cap_current"]
    if "contract_row_id" in resolver_results.columns and "contract_row_id" in raw_contracts.columns:
        return raw_contracts.merge(resolver_results[["contract_row_id"] + resolver_cols], on="contract_row_id", how="left")
    # Fallback: concat columns
    for col in resolver_cols:
        if col not in raw_contracts.columns:
            raw_contracts = raw_contracts.copy()
            raw_contracts[col] = resolver_results[col].values if col in resolver_results.columns else None
    return raw_contracts

def write_output(output_df: pd.DataFrame, path: str, fmt: str = "parquet") -> None:
    from resolver.persistence import write_parquet
    if fmt == "parquet":
        write_parquet(output_df, path)
    elif fmt == "csv":
        output_df.to_csv(path, index=False)
    elif fmt == "json":
        output_df.to_json(path, orient="records", lines=True)
    else:
        raise ValueError(f"Unsupported output format: {fmt}")

def write_diagnostics(diag_df: pd.DataFrame, path: str) -> None:
    from resolver.persistence import write_parquet
    write_parquet(diag_df, path)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _score_to_confidence(score: float) -> str:
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium_high"
    if score >= 45:
        return "medium"
    if score >= 30:
        return "low_medium"
    return "low"
