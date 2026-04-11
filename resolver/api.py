"""resolver/api.py — Public entry points. No heavy logic here."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from resolver.models import load_config, ResolverConfig, ReferenceHandles, RefreshReport

log = logging.getLogger(__name__)

# ── resolve_contracts ─────────────────────────────────────────────────────────

def resolve_contracts(
    contracts,
    config=None,
    reference_handles: ReferenceHandles | None = None,
    overrides: dict | None = None,
    return_format: str = "pandas",
    include_diagnostics: bool = True,
    strict_mode: bool = True,
):
    """
    Main batch entry point.

    contracts: DataFrame | path-to-CSV | path-to-Parquet
    config:    ResolverConfig | dict | None
    reference_handles: pre-built ReferenceHandles (reuse across calls)
    overrides: dict[entity_key → OverrideRecord]
    return_format: "pandas" | "polars" | "dict"
    """
    import pandas as pd
    from resolver.ingest import load_contracts
    from resolver.reference import build_reference_handles
    from resolver.orchestration import resolve_contract_tickers
    from resolver.persistence import ensure_dirs, init_sqlite_cache
    from resolver.qa import build_resolution_diagnostics

    cfg = load_config(config)

    try:
        ensure_dirs(cfg)
        init_sqlite_cache(cfg)
    except Exception as e:
        if strict_mode:
            raise
        log.warning(f"Storage init failed (strict_mode=False): {e}")

    # Load reference data
    if reference_handles is None:
        try:
            reference_handles = build_reference_handles(cfg)
        except Exception as e:
            if strict_mode:
                raise
            log.warning(f"Reference data load failed: {e}. Continuing with empty handles.")
            reference_handles = ReferenceHandles()

    # Load and canonicalize contracts
    try:
        df = load_contracts(contracts)
    except Exception as e:
        if strict_mode:
            raise
        log.error(f"Contract load failed: {e}")
        import pandas as pd
        return pd.DataFrame()

    # Run resolution pipeline
    try:
        result_df = resolve_contract_tickers(df, reference_handles, cfg, overrides, include_diagnostics)
    except Exception as e:
        if strict_mode:
            raise
        log.error(f"Resolution pipeline failed: {e}")
        result_df = df.copy()
        result_df["resolver_status"] = "null_error"
        result_df["resolver_ticker"] = None

    # Diagnostics
    if include_diagnostics:
        diag = build_resolution_diagnostics(result_df, cfg)
        log.info(
            f"Resolution complete: {diag.get('resolved', 0):,}/{diag.get('total', 0):,} resolved "
            f"({diag.get('resolution_rate', 0):.1%})"
        )

    return _format_output(result_df, return_format)

# ── resolve_entities ──────────────────────────────────────────────────────────

def resolve_entities(
    awardee_entities,
    parent_entities=None,
    config=None,
    reference_handles: ReferenceHandles | None = None,
    overrides: dict | None = None,
    include_diagnostics: bool = True,
    strict_mode: bool = True,
) -> dict:
    """
    Entity-level entry point when the caller already deduplicated vendors.
    awardee_entities: list[AwardeeEntity] or dict[key, AwardeeEntity]
    parent_entities:  list[ParentEntity]  or dict[key, ParentEntity] or None
    Returns dict[entity_key → resolution_metadata].
    """
    from resolver.models import AwardeeEntity, ParentEntity
    from resolver.reference import build_reference_handles
    from resolver.orchestration import resolve_all_entities
    from resolver.overrides import load_overrides

    cfg = load_config(config)
    if reference_handles is None:
        reference_handles = build_reference_handles(cfg)

    # Normalise inputs to dicts
    if isinstance(awardee_entities, list):
        awardee_dict = {e.entity_key: e for e in awardee_entities}
    else:
        awardee_dict = awardee_entities or {}

    if isinstance(parent_entities, list):
        parent_dict = {e.entity_key: e for e in parent_entities}
    else:
        parent_dict = parent_entities or {}

    from resolver.entities import build_entity_resolution_inputs
    entity_inputs_list = build_entity_resolution_inputs(awardee_dict, parent_dict)
    entity_inputs = {"graph": {"awardees": awardee_dict, "parents": parent_dict}, "inputs": entity_inputs_list}

    if overrides is None:
        try:
            overrides = load_overrides(cfg)
        except Exception:
            overrides = {}

    return resolve_all_entities(entity_inputs, reference_handles, cfg, overrides)

# ── refresh_reference_data ────────────────────────────────────────────────────

def refresh_reference_data(
    config=None,
    sources: str | list[str] | None = None,
    force: bool = False,
    since=None,
    max_concurrency: int | None = None,
) -> list[RefreshReport]:
    """
    Refresh local reference datasets.
    sources: "sec" | "gleif" | "gleif_isin" | "sam_optional" | "all" | list of above
    """
    from resolver.orchestration import (
        refresh_sec, refresh_gleif, refresh_isin_lei, refresh_sam, rebuild_reference_masters
    )
    from resolver.persistence import ensure_dirs, init_sqlite_cache

    cfg = load_config(config)
    ensure_dirs(cfg)
    init_sqlite_cache(cfg)

    if sources is None or sources == "all":
        sources = ["sec", "gleif", "gleif_isin", "sam_optional"]
    elif isinstance(sources, str):
        sources = [sources]

    reports = []
    for source in sources:
        try:
            if source == "sec":
                reports.append(refresh_sec(cfg, force))
            elif source == "gleif":
                reports.append(refresh_gleif(cfg, force))
            elif source == "gleif_isin":
                reports.append(refresh_isin_lei(cfg, force))
            elif source == "sam_optional":
                reports.append(refresh_sam(cfg, force))
            else:
                log.warning(f"Unknown refresh source: {source}")
        except Exception as e:
            log.error(f"Refresh failed for {source}: {e}")
            from datetime import datetime
            reports.append(RefreshReport(source, datetime.utcnow(), datetime.utcnow(), "failed", error=str(e)))

    rebuild_reference_masters(cfg)
    return reports

# ── explain_resolution ────────────────────────────────────────────────────────

def explain_resolution(
    resolution_id: str,
    store: dict | None = None,
    fmt: str = "dict",
) -> dict | str:
    """
    Return a human-readable explanation for one resolved row.
    store: dict[resolution_id → row_dict] from the resolved output DataFrame.
    fmt: "dict" | "text"
    """
    from resolver.qa import build_explanation_record, format_explanation_as_dict, format_explanation_as_text
    rec = build_explanation_record(resolution_id, config=None, store=store)
    if fmt == "text":
        return format_explanation_as_text(rec)
    return format_explanation_as_dict(rec)

# ── Output formatting ─────────────────────────────────────────────────────────

def _format_output(df, return_format: str):
    if return_format == "pandas":
        return df
    if return_format == "polars":
        try:
            import polars as pl
            return pl.from_pandas(df)
        except ImportError:
            log.warning("polars not installed, returning pandas DataFrame")
            return df
    if return_format == "dict":
        return df.to_dict(orient="records")
    return df


# ── resolve_v1 ────────────────────────────────────────────────────────────────

def resolve_v1(
    contracts,
    db_path:   str  = "data/cache/resolver.duckdb",
    cache_dir: str  = "data/cache",
    config:    dict | None = None,
    refresh:   bool = False,
) -> "pd.DataFrame":
    """
    V1 pipeline entry point.
    contracts: DataFrame | path-to-CSV | path-to-Parquet
    Returns DataFrame with V1 output columns.
    """
    import json
    import pandas as pd
    from resolver.storage import get_db, ensure_schema
    from resolver.issuer_master import refresh_issuer_master, get_issuer_master_version
    from resolver.clusters import build_entity_clusters
    from resolver.pipeline import (ClusterContext, resolve_cluster,
                                   flush_resolution_cache_pending,
                                   invalidate_resolution_cache,
                                   invalidate_alias_index)
    from resolver.models import V1ThresholdsConfig
    from resolver.normalize import conservative_normalize
    from resolver.ingest import load_contracts, assign_contract_row_ids, build_contract_identity_features

    con = get_db(db_path)
    ensure_schema(con)
    if refresh:
        im_version = refresh_issuer_master(con, cache_dir, force=True)
        invalidate_alias_index()       # issuer_master/aliases rebuilt — drop stale index
        invalidate_resolution_cache()  # old cache entries may reference stale issuers
    else:
        im_version = get_issuer_master_version(con)

    thresholds = V1ThresholdsConfig()
    if config:
        for k, v in config.get("thresholds", {}).items():
            if hasattr(thresholds, k):
                setattr(thresholds, k, v)

    df             = load_contracts(contracts)
    df             = assign_contract_row_ids(df)
    features       = build_contract_identity_features(df)
    row_to_cluster = build_entity_clusters(features, con)

    current_cluster_ids = set(row_to_cluster.values())
    placeholders = ", ".join("?" * len(current_cluster_ids))
    clusters = con.execute(
        "SELECT entity_cluster_id, canonical_parent_name, canonical_entity_name, "
        "ultimate_parent_uei, uei, cage_code, all_parent_names_json, "
        f"all_legal_names_json, all_dba_names_json FROM entity_clusters "
        f"WHERE entity_cluster_id IN ({placeholders})",
        list(current_cluster_ids),
    ).fetchall()

    cluster_results: dict[str, dict] = {}
    for row in clusters:
        cid, cp, ce, puei, uei, cage, pj, lj, dj = row
        ctx = ClusterContext(
            cluster_id=cid, canonical_parent_name=cp, canonical_entity_name=ce,
            uei=uei, parent_uei=puei, cage=cage,
            parent_name_norm=conservative_normalize(cp),
            legal_name_norm=conservative_normalize(ce),
            all_parent_names=json.loads(pj or "[]"),
            all_legal_names=json.loads(lj or "[]"),
            all_dba_names=json.loads(dj or "[]"),
        )
        cluster_results[cid] = resolve_cluster(ctx, con, thresholds, im_version)

    # Flush all queued resolution cache writes in one bulk operation
    flush_resolution_cache_pending(con)

    # Join resolution results back to contract rows (vectorized — no iterrows)
    df["_cluster_id"] = df["contract_row_id"].map(row_to_cluster)
    res_df = pd.DataFrame(list(cluster_results.values()))
    if not res_df.empty:
        res_df = res_df.rename(columns={"entity_cluster_id": "_cluster_id"})
        result = df.merge(res_df, on="_cluster_id", how="left")
    else:
        result = df.copy()
    result = result.drop(columns=["_cluster_id"], errors="ignore")
    return result
