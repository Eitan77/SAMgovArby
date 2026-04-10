"""resolver/_compat.py — Backward-compatibility shim for ticker_resolver_v4 callers.

Provides TickerResolverV4 and resolve_ticker with the same interface as the old module,
delegating to the new resolver package internally.
"""
from __future__ import annotations

import logging
from datetime import date

log = logging.getLogger(__name__)

# ── TickerResolverV4 shim ─────────────────────────────────────────────────────

class TickerResolverV4:
    """
    Drop-in replacement for the old TickerResolverV4 class.
    Accepts the same constructor args; delegates to the new resolver pipeline.
    """

    def __init__(
        self,
        edgar_map: dict | None = None,
        cache_path: str = ".ticker_cache_v4.json",
        mcap_cache_path: str | None = None,
        gleif_name_search: bool = False,
    ):
        from resolver.models import load_config, ReferenceHandles
        from resolver.reference import build_reference_handles
        from resolver.persistence import ensure_dirs, init_sqlite_cache

        self.config = load_config()
        # SEC live fallback: only fires for entities that scored below threshold from the
        # static EDGAR map — already-resolved entities are never hit. Cost: ~0.12s/req
        # for low-score entities only (~8-10min overhead on a full 12k-entity build).
        self.config.source_policies.allow_sec_live_fallback = True
        self.config.source_policies.allow_gleif_api_fallback = False
        # Override cache paths if caller specified them
        if mcap_cache_path:
            self.config.paths.cache_dir = mcap_cache_path.rsplit("/", 1)[0] or self.config.paths.cache_dir

        ensure_dirs(self.config)
        init_sqlite_cache(self.config)

        # If caller passed a pre-built edgar_map, inject it
        if edgar_map is not None:
            self._references = ReferenceHandles(edgar_map=edgar_map)
            from resolver.reference import (
                create_sec_issuer_master, create_sec_alias_table, create_sec_filing_locator,
            )
            self._references.sec_issuer_master = create_sec_issuer_master(edgar_map, self.config)
            self._references.sec_alias_table   = create_sec_alias_table(
                self._references.sec_issuer_master, edgar_map
            )
        else:
            try:
                self._references = build_reference_handles(self.config)
            except Exception as e:
                log.warning(f"Could not build reference handles: {e}. Using empty handles.")
                self._references = ReferenceHandles()

        self._overrides: dict = {}
        try:
            from resolver.overrides import load_overrides
            self._overrides = load_overrides(self.config)
        except Exception:
            pass

        self.cache: dict = {}           # kept for API compatibility
        self._mem_cache: dict = {}      # in-memory entity key → result dict (avoids SQLite per-entity)

    def resolve(self, record) -> dict:
        """
        Resolve a ContractRecord → result dict.
        The result dict uses the new resolver output schema but also includes
        legacy fields (resolved_ticker, confidence, evidence_type, etc.).
        """
        from resolver.ingest import build_contract_identity_features
        from resolver.entities import build_awardee_entities, build_parent_entities, choose_parent_entity_key
        from resolver.orchestration import resolve_all_entities
        from resolver.models import ContractIdentityFeatures
        from resolver.normalize import normalize_name, normalize_uei, normalize_cage, extract_registered_domain, normalize_country
        from decimal import Decimal
        from datetime import datetime

        # Build a single ContractIdentityFeatures from the ContractRecord
        feat = ContractIdentityFeatures(
            contract_row_id            = record.piid or "compat_row",
            award_date                 = None,
            awardee_uei                = normalize_uei(getattr(record, "uei", None)),
            parent_uei                 = None,
            cage_code                  = normalize_cage(getattr(record, "cage_code", None)),
            awardee_name_raw           = getattr(record, "contractor_name", "") or getattr(record, "legal_business_name", ""),
            awardee_name_norm          = normalize_name(getattr(record, "contractor_name", "") or getattr(record, "legal_business_name", "")),
            awardee_dba_raw            = getattr(record, "dba_name", "") or None,
            awardee_dba_norm           = normalize_name(getattr(record, "dba_name", "")),
            parent_name_raw            = getattr(record, "parent_name", "") or None,
            parent_name_norm           = normalize_name(getattr(record, "parent_name", "")),
            website_raw                = None,
            website_domain             = None,
            vendor_city_norm           = None,
            vendor_state_norm          = None,
            vendor_zip_norm            = None,
            vendor_country_norm        = normalize_country(getattr(record, "country_of_incorporation", "USA")),
            incorporation_country_norm = normalize_country(getattr(record, "country_of_incorporation", "USA")),
            phone_norm                 = None,
            dollars_obligated          = Decimal(str(getattr(record, "award_amount", 0) or 0)),
        )

        # Build entity graph for this single record
        features = [feat]
        awardees = build_awardee_entities(features, self.config)
        parents  = build_parent_entities(features, self.config)
        from resolver.entities import link_awardees_to_parents, build_entity_resolution_inputs, choose_awardee_entity_key as _cak
        link_awardees_to_parents(awardees, parents, features)

        # In-memory cache check — avoids rebuilding for already-resolved entities
        mem_key = _cak(feat)
        if mem_key in self._mem_cache:
            return self._mem_cache[mem_key]

        entity_inputs = {
            "graph":  {"awardees": awardees, "parents": parents},
            "inputs": build_entity_resolution_inputs(awardees, parents),
        }

        decisions = resolve_all_entities(entity_inputs, self._references, self.config, self._overrides)

        # Pick the decision for this entity
        from resolver.entities import choose_awardee_entity_key
        entity_key = mem_key
        meta = decisions.get(entity_key, {})

        # Build legacy-compatible result dict
        ticker     = meta.get("resolver_ticker")
        confidence = meta.get("resolver_confidence", "none")
        status     = meta.get("resolver_status", "unresolved")

        result = {
            # Legacy fields
            "original_name":      feat.awardee_name_raw,
            "normalized_name":    feat.awardee_name_norm,
            "resolved_ticker":    ticker,
            "resolved_cik":       meta.get("resolver_matched_cik") or "",
            "evidence_type":      meta.get("resolver_entity_match_source") or "none",
            "confidence":         confidence,
            "rejection_reason":   meta.get("resolver_null_reason"),
            "market_cap_current": meta.get("market_cap_current", 0.0),
            "audit_trail":        [],
            "last_verified":      datetime.utcnow().isoformat(),
            # New fields also included
            **{k: v for k, v in meta.items() if k.startswith("resolver_")},
        }
        # Store in memory cache for deduplication
        self._mem_cache[mem_key] = result
        return result

    def save_cache(self) -> None:
        """No-op: new resolver uses SQLite/Parquet persistence automatically."""
        pass

    def _get_market_cap(self, ticker: str) -> float:
        from resolver.historical import get_market_cap
        return get_market_cap(ticker, self._references.security_cache)


# ── Module-level resolve_ticker shim ─────────────────────────────────────────

_resolver_instance: TickerResolverV4 | None = None


def resolve_ticker(
    awardee_name: str,
    edgar_results=None,
    resolver: TickerResolverV4 | None = None,
    cage_code: str = "",
) -> tuple[str | None, str]:
    """
    Drop-in replacement for the old module-level resolve_ticker().
    Returns (ticker_or_None, confidence_str).
    """
    global _resolver_instance
    if resolver is None:
        if _resolver_instance is None:
            _resolver_instance = TickerResolverV4()
        resolver = _resolver_instance

    # Build a minimal ContractRecord-like object
    from sam_gov_reader import ContractRecord
    record = ContractRecord(
        piid="",
        cage_code=cage_code or "",
        uei="",
        country_of_incorporation="USA",
        contractor_name=awardee_name or "",
        legal_business_name=awardee_name or "",
        dba_name="",
        parent_name="",
        parent_uei="",
        award_amount=0.0,
        posted_date="",
        agency="",
        naics_code="",
        naics_description="",
        set_aside_code="",
        extent_competed_code="",
        other_than_full_open="",
        idv_type="",
        num_offers="",
        is_educational_institution=False,
        is_federal_agency=False,
        is_airport_authority=False,
        is_council_of_governments=False,
        is_community_dev_corp=False,
        is_federally_funded_rd=False,
    )
    result = resolver.resolve(record)
    return result.get("resolved_ticker"), result.get("confidence", "none")
