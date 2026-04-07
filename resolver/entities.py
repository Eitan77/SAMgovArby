"""resolver/entities.py — Awardee/parent entity deduplication, graph, aggregation."""
from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any

from resolver.models import (
    AwardeeEntity, ParentEntity, ContractIdentityFeatures,
)
from resolver.normalize import normalize_name, strip_legal_suffixes, build_address_signature

log = logging.getLogger(__name__)

# ── Awardee entity keying ─────────────────────────────────────────────────────

def choose_awardee_entity_key(feat: ContractIdentityFeatures) -> str:
    """Priority: UEI > CAGE > synthetic(name + address)."""
    if feat.awardee_uei:
        return f"uei:{feat.awardee_uei}"
    if feat.cage_code:
        return f"cage:{feat.cage_code}"
    addr_sig = build_address_signature(
        feat.vendor_city_norm, feat.vendor_state_norm,
        feat.vendor_zip_norm, feat.vendor_country_norm,
    ) or ""
    name_key = feat.awardee_name_norm or "unknown"
    raw      = f"syn:{name_key}|{addr_sig}"
    return "syn:" + hashlib.md5(raw.encode()).hexdigest()[:12]

def choose_parent_entity_key(feat: ContractIdentityFeatures) -> str | None:
    """Priority: parent_uei > synthetic(parent_name + country)."""
    if not feat.parent_name_raw:
        return None
    if feat.parent_uei:
        return f"puei:{feat.parent_uei}"
    name_key = feat.parent_name_norm or "unknown"
    country  = feat.vendor_country_norm or ""
    raw      = f"syn:{name_key}|{country}"
    return "psyn:" + hashlib.md5(raw.encode()).hexdigest()[:12]

# ── Awardee entity builder ────────────────────────────────────────────────────

def build_awardee_entities(features: list[ContractIdentityFeatures], config) -> dict[str, AwardeeEntity]:
    """Deduplicate contract rows into AwardeeEntity records, keyed by entity_key."""
    groups: dict[str, list[ContractIdentityFeatures]] = defaultdict(list)
    for feat in features:
        key = choose_awardee_entity_key(feat)
        groups[key].append(feat)

    entities: dict[str, AwardeeEntity] = {}
    for key, rows in groups.items():
        entity = _aggregate_awardee_group(key, rows)
        entities[key] = entity
    log.debug(f"Built {len(entities):,} awardee entities from {len(features):,} contract rows")
    return entities

def _aggregate_awardee_group(entity_key: str, rows: list[ContractIdentityFeatures]) -> AwardeeEntity:
    key_type: Any = "synthetic"
    if entity_key.startswith("uei:"):
        key_type = "uei"
    elif entity_key.startswith("cage:"):
        key_type = "cage"

    uei       = next((r.awardee_uei for r in rows if r.awardee_uei), None)
    cage      = next((r.cage_code for r in rows if r.cage_code), None)

    # Canonical name: most frequent non-null
    name_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        if r.awardee_name_raw:
            name_counts[r.awardee_name_raw] += 1
    canonical_name = max(name_counts, key=name_counts.__getitem__) if name_counts else None
    canonical_norm = normalize_name(canonical_name)

    # Collect all unique names (legal + DBA)
    alias_set: set[str] = set()
    for r in rows:
        if r.awardee_name_raw:
            alias_set.add(r.awardee_name_raw)
        if r.awardee_dba_raw:
            alias_set.add(r.awardee_dba_raw)
    if canonical_name:
        alias_set.discard(canonical_name)

    # Domains
    domain_set: set[str] = {r.website_domain for r in rows if r.website_domain}

    # Addresses
    addr_set: set[str] = set()
    addresses = []
    for r in rows:
        sig = build_address_signature(
            r.vendor_city_norm, r.vendor_state_norm, r.vendor_zip_norm, r.vendor_country_norm
        )
        if sig and sig not in addr_set:
            addr_set.add(sig)
            addresses.append({
                "city": r.vendor_city_norm, "state": r.vendor_state_norm,
                "zip": r.vendor_zip_norm,   "country": r.vendor_country_norm,
            })

    # Dates
    dates = sorted(r.award_date for r in rows if r.award_date)
    first_seen = dates[0]  if dates else None
    last_seen  = dates[-1] if dates else None

    # Obligated total
    total = sum(r.dollars_obligated for r in rows if r.dollars_obligated)
    total = total or None

    return AwardeeEntity(
        entity_key          = entity_key,
        source_key_type     = key_type,
        uei                 = uei,
        cage_code           = cage,
        canonical_name      = canonical_name,
        canonical_name_norm = canonical_norm,
        alias_names         = sorted(alias_set),
        domains             = sorted(domain_set),
        addresses           = addresses,
        linked_parent_keys  = [],  # filled in by link_awardees_to_parents
        first_seen_date     = first_seen,
        last_seen_date      = last_seen,
        contract_count      = len(rows),
        total_obligated     = total,
    )

# ── Parent entity builder ─────────────────────────────────────────────────────

def build_parent_entities(features: list[ContractIdentityFeatures], config) -> dict[str, ParentEntity]:
    """Deduplicate parent rows into ParentEntity records."""
    groups: dict[str, list[ContractIdentityFeatures]] = defaultdict(list)
    for feat in features:
        key = choose_parent_entity_key(feat)
        if key:
            groups[key].append(feat)

    entities: dict[str, ParentEntity] = {}
    for key, rows in groups.items():
        entity = _aggregate_parent_group(key, rows)
        entities[key] = entity
    log.debug(f"Built {len(entities):,} parent entities")
    return entities

def _aggregate_parent_group(entity_key: str, rows: list[ContractIdentityFeatures]) -> ParentEntity:
    key_type: Any = "synthetic"
    if entity_key.startswith("puei:"):
        key_type = "parent_uei"

    parent_uei = next((r.parent_uei for r in rows if r.parent_uei), None)

    name_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        if r.parent_name_raw:
            name_counts[r.parent_name_raw] += 1
    canonical_name = max(name_counts, key=name_counts.__getitem__) if name_counts else None
    canonical_norm = normalize_name(canonical_name)

    alias_set: set[str] = set()
    for r in rows:
        if r.parent_name_raw:
            alias_set.add(r.parent_name_raw)
    if canonical_name:
        alias_set.discard(canonical_name)

    domain_set:   set[str] = {r.website_domain for r in rows if r.website_domain}
    country_set:  set[str] = {r.vendor_country_norm for r in rows if r.vendor_country_norm}

    dates      = sorted(r.award_date for r in rows if r.award_date)
    first_seen = dates[0]  if dates else None
    last_seen  = dates[-1] if dates else None
    total      = sum(r.dollars_obligated for r in rows if r.dollars_obligated) or None

    return ParentEntity(
        entity_key          = entity_key,
        source_key_type     = key_type,
        parent_uei          = parent_uei,
        canonical_name      = canonical_name,
        canonical_name_norm = canonical_norm,
        alias_names         = sorted(alias_set),
        domains             = sorted(domain_set),
        countries           = sorted(country_set),
        linked_awardee_keys = [],
        first_seen_date     = first_seen,
        last_seen_date      = last_seen,
        contract_count      = len(rows),
        total_obligated     = total,
    )

# ── Graph: link awardees ↔ parents ────────────────────────────────────────────

def link_awardees_to_parents(
    awardee_entities: dict[str, AwardeeEntity],
    parent_entities:  dict[str, ParentEntity],
    features:         list[ContractIdentityFeatures],
) -> None:
    """Populate linked_parent_keys on awardees and linked_awardee_keys on parents (in-place)."""
    for feat in features:
        akey = choose_awardee_entity_key(feat)
        pkey = choose_parent_entity_key(feat)
        if not pkey:
            continue
        ae = awardee_entities.get(akey)
        pe = parent_entities.get(pkey)
        if ae and pkey not in ae.linked_parent_keys:
            ae.linked_parent_keys.append(pkey)
        if pe and akey not in pe.linked_awardee_keys:
            pe.linked_awardee_keys.append(akey)

def build_entity_graph(features: list[ContractIdentityFeatures], config) -> dict:
    """Full entity graph build: awardees + parents + links."""
    awardees = build_awardee_entities(features, config)
    parents  = build_parent_entities(features, config)
    link_awardees_to_parents(awardees, parents, features)
    return {"awardees": awardees, "parents": parents}

def fan_entity_keys_to_contract_rows(
    features:         list[ContractIdentityFeatures],
    awardee_entities: dict[str, AwardeeEntity],
    parent_entities:  dict[str, ParentEntity],
) -> list[dict]:
    """Attach entity_key and parent_entity_key to each contract row feature dict."""
    result = []
    for feat in features:
        akey = choose_awardee_entity_key(feat)
        pkey = choose_parent_entity_key(feat)
        result.append({
            "contract_row_id":  feat.contract_row_id,
            "entity_key":       akey,
            "parent_entity_key": pkey,
        })
    return result

# ── Aggregation helpers ───────────────────────────────────────────────────────

def choose_canonical_name(alias_counts: dict[str, int]) -> str | None:
    """Pick the most frequent name; prefer legal over DBA."""
    if not alias_counts:
        return None
    return max(alias_counts, key=alias_counts.__getitem__)

def choose_canonical_domain(domain_counts: dict[str, int]) -> str | None:
    if not domain_counts:
        return None
    return max(domain_counts, key=domain_counts.__getitem__)

def aggregate_entity_history(rows: list[dict]) -> dict:
    """Summarize a group of contract rows: date range, total obligated."""
    dates  = sorted(r["award_date"] for r in rows if r.get("award_date"))
    total  = sum(float(r.get("dollars_obligated") or 0) for r in rows)
    return {
        "contract_count":  len(rows),
        "first_seen_date": str(dates[0])  if dates else None,
        "last_seen_date":  str(dates[-1]) if dates else None,
        "total_obligated": total,
    }

def build_entity_resolution_inputs(
    awardee_entities: dict[str, AwardeeEntity],
    parent_entities:  dict[str, ParentEntity],
) -> list[dict]:
    """Convert entities into flat dicts ready for the matching layer."""
    inputs = []
    for key, ae in awardee_entities.items():
        parent_key = ae.linked_parent_keys[0] if ae.linked_parent_keys else None
        pe         = parent_entities.get(parent_key) if parent_key else None
        inputs.append({
            "entity_key":        key,
            "entity_type":       "awardee",
            "awardee":           ae,
            "parent":            pe,
            "parent_entity_key": parent_key,
        })
    return inputs
