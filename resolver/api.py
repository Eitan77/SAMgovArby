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
