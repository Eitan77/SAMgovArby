"""resolver/matching.py — Null classifier, candidate generation, scoring, parent-first, decision."""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any

from rapidfuzz import fuzz, process

from resolver.models import (
    IssuerCandidate, EntityResolutionDecision, AwardeeEntity, ParentEntity,
    ReferenceHandles, ResolverConfig, KNOWN_ALIASES, NOT_COMPETED_CODES,
)
from resolver.normalize import (
    normalize_name, strip_legal_suffixes, name_tokens, is_non_public_name,
    looks_like_government_entity, looks_like_nonprofit_or_university,
    address_similarity, build_address_signature,
)
from resolver.reference import (
    lookup_sec_candidates_by_name, lookup_gleif_candidates, traverse_parent_chain,
    lookup_substr_candidates,
)
from resolver.ingest import fetch_sec_entity_metadata, gleif_name_search, gleif_lei_lookup, gleif_get_parent_lei, openfigi_lei_to_ticker

log = logging.getLogger(__name__)

# ── Null classifier ───────────────────────────────────────────────────────────

def classify_obvious_null_entity(entity: AwardeeEntity | ParentEntity, config) -> tuple[bool, str | None]:
    """
    High-precision null detection before any API calls.
    Returns (is_null, reason_str).
    """
    name = entity.canonical_name or ""
    norm = entity.canonical_name_norm or ""

    if not name.strip():
        return True, "bad_input"

    # Foreign incorporation (country on awardee)
    if isinstance(entity, AwardeeEntity):
        for addr in getattr(entity, "addresses", []):
            country = addr.get("country", "")
            if country and country not in ("USA", "US"):
                return True, "foreign_entity"

    # Non-public name patterns
    if is_non_public_name(name):
        return True, "non_public_entity"

    if looks_like_government_entity(name):
        return True, "non_public_entity"

    if looks_like_nonprofit_or_university(name):
        return True, "non_public_entity"

    return False, None

# ── Known alias fast path ─────────────────────────────────────────────────────

def check_known_aliases(entity: AwardeeEntity | ParentEntity) -> str | None:
    """Check KNOWN_ALIASES for an instant ticker match. Returns ticker or None."""
    names_to_try = [entity.canonical_name] + list(entity.alias_names)
    for raw_name in names_to_try:
        if not raw_name:
            continue
        norm    = normalize_name(raw_name)
        stripped = strip_legal_suffixes(norm) if norm else None
        for key in filter(None, [stripped, norm]):
            if key in KNOWN_ALIASES:
                return KNOWN_ALIASES[key]
    return None

# ── SEC candidate generation ──────────────────────────────────────────────────

def generate_sec_candidates(
    entity: AwardeeEntity | ParentEntity,
    references: ReferenceHandles,
    config: ResolverConfig,
    entity_level: str = "awardee",
) -> list[IssuerCandidate]:
    """Generate issuer candidates from SEC alias table + edgar_map."""
    candidates: list[IssuerCandidate] = []
    names_to_try = _entity_search_names(entity)
    seen_ciks: set[str] = set()

    for name in names_to_try:
        norm    = normalize_name(name) or ""
        stripped = strip_legal_suffixes(norm)
        for search_norm in {norm, stripped}:
            if not search_norm:
                continue
            ciks = lookup_sec_candidates_by_name(search_norm, references.sec_alias_table)
            for cik in ciks:
                if cik in seen_ciks:
                    continue
                seen_ciks.add(cik)
                issuer = references.sec_issuer_master.get(cik)
                if not issuer:
                    # Try edgar_map
                    for ename, entry in references.edgar_map.items():
                        if entry.get("cik", "").zfill(10) == cik.zfill(10):
                            issuer = {"name": ename, "tickers": [entry["ticker"]], "cik": cik}
                            break
                if not issuer:
                    continue
                issuer_name = issuer.get("name", "")
                cand = IssuerCandidate(
                    candidate_id     = str(uuid.uuid4())[:8],
                    source           = "sec_exact",
                    issuer_key       = f"cik:{cik}",
                    issuer_name      = issuer_name,
                    issuer_name_norm = normalize_name(issuer_name) or "",
                    cik              = cik,
                    lei              = None,
                    figi             = None,
                    match_level      = "exact",
                    entity_level     = entity_level,
                    score_total      = 0.0,
                )
                candidates.append(cand)

    # Fuzzy candidates from edgar_map names
    if len(candidates) < 3:
        fuzzy_cands = _generate_fuzzy_sec_candidates(entity, references, config, entity_level, seen_ciks)
        candidates.extend(fuzzy_cands)

    return candidates[:50]  # cap

def _generate_fuzzy_sec_candidates(
    entity, references, config, entity_level, seen_ciks
) -> list[IssuerCandidate]:
    """Rapidfuzz top-N matches against EDGAR company names."""
    names_to_try = _entity_search_names(entity)
    edgar_names  = list(references.edgar_map.keys())
    if not edgar_names:
        return []
    results_out: list[IssuerCandidate] = []
    for name in names_to_try[:2]:  # only first two names to keep it fast
        norm    = normalize_name(name)
        stripped = strip_legal_suffixes(norm) if norm else None
        for query in filter(None, [norm, stripped]):
            matches = process.extract(query, edgar_names, scorer=fuzz.token_sort_ratio, limit=5)
            for match_name, score, _ in matches:
                min_score = 70 if len((stripped or "").split()) <= 3 else 75
                if score < min_score:
                    continue
                entry = references.edgar_map.get(match_name, {})
                cik   = entry.get("cik", "").zfill(10) if entry.get("cik") else ""
                if cik in seen_ciks:
                    continue
                seen_ciks.add(cik)
                cand = IssuerCandidate(
                    candidate_id     = str(uuid.uuid4())[:8],
                    source           = "sec_exact",
                    issuer_key       = f"cik:{cik}" if cik else f"name:{match_name}",
                    issuer_name      = match_name,
                    issuer_name_norm = normalize_name(match_name) or "",
                    cik              = cik or None,
                    lei              = None,
                    figi             = None,
                    match_level      = "fuzzy",
                    entity_level     = entity_level,
                    score_total      = 0.0,
                    supporting_evidence = {"fuzzy_score": score},
                )
                results_out.append(cand)
    return results_out

# ── GLEIF candidate generation ────────────────────────────────────────────────

def generate_gleif_candidates(
    entity: AwardeeEntity | ParentEntity,
    references: ReferenceHandles,
    config: ResolverConfig,
    entity_level: str = "awardee",
) -> list[IssuerCandidate]:
    """Generate issuer candidates from GLEIF alias table or live API."""
    candidates: list[IssuerCandidate] = []
    names_to_try = _entity_search_names(entity)
    seen_leis: set[str] = set()

    for name in names_to_try[:3]:
        norm    = normalize_name(name) or ""
        stripped = strip_legal_suffixes(norm)
        # Try local GLEIF alias table first
        for search_norm in {norm, stripped}:
            leis = lookup_gleif_candidates(search_norm, references.gleif_alias_table)
            for lei in leis:
                if lei in seen_leis:
                    continue
                seen_leis.add(lei)
                gleif_entity = references.gleif_entity_master.get(lei, {})
                if not gleif_entity:
                    continue
                issuer_name = gleif_entity.get("name", "")
                cand = IssuerCandidate(
                    candidate_id     = str(uuid.uuid4())[:8],
                    source           = "gleif_direct",
                    issuer_key       = f"lei:{lei}",
                    issuer_name      = issuer_name,
                    issuer_name_norm = normalize_name(issuer_name) or "",
                    cik              = None,
                    lei              = lei,
                    figi             = None,
                    match_level      = "exact",
                    entity_level     = entity_level,
                    score_total      = 0.0,
                )
                candidates.append(cand)

    # Live GLEIF API fallback when local table empty
    if not candidates and config.source_policies.allow_gleif_api_fallback:
        primary_name = entity.canonical_name or ""
        stripped_name = strip_legal_suffixes(normalize_name(primary_name) or "")
        if stripped_name:
            api_results = gleif_name_search(stripped_name, config)
            for r in api_results[:5]:
                lei = r.get("lei")
                if not lei or lei in seen_leis:
                    continue
                seen_leis.add(lei)
                cand = IssuerCandidate(
                    candidate_id     = str(uuid.uuid4())[:8],
                    source           = "gleif_direct",
                    issuer_key       = f"lei:{lei}",
                    issuer_name      = r.get("name", ""),
                    issuer_name_norm = normalize_name(r.get("name", "")) or "",
                    cik              = None,
                    lei              = lei,
                    figi             = None,
                    match_level      = "api_search",
                    entity_level     = entity_level,
                    score_total      = 0.0,
                )
                candidates.append(cand)

    return candidates[:20]

def generate_parent_chain_candidates(
    entity: AwardeeEntity | ParentEntity,
    references: ReferenceHandles,
    config: ResolverConfig,
    entity_level: str = "awardee",
) -> list[IssuerCandidate]:
    """Walk GLEIF parent chain and generate SEC candidates for each parent LEI."""
    candidates: list[IssuerCandidate] = []
    # First get GLEIF candidates for this entity to obtain LEIs
    gleif_cands = generate_gleif_candidates(entity, references, config, entity_level)
    for gc in gleif_cands[:3]:
        if not gc.lei:
            continue
        chain = traverse_parent_chain(gc.lei, references.gleif_relationships, config)
        for chain_item in chain:
            parent_lei = chain_item["lei"]
            parent_entity_data = references.gleif_entity_master.get(parent_lei, {})
            parent_name = parent_entity_data.get("name", "")
            if not parent_name:
                continue
            # Try to find SEC match for this parent
            parent_norm = normalize_name(parent_name) or ""
            ciks = lookup_sec_candidates_by_name(parent_norm, references.sec_alias_table, limit=3)
            for cik in ciks:
                issuer = references.sec_issuer_master.get(cik, {})
                issuer_name = issuer.get("name", parent_name)
                cand = IssuerCandidate(
                    candidate_id     = str(uuid.uuid4())[:8],
                    source           = "gleif_parent_chain",
                    issuer_key       = f"cik:{cik}",
                    issuer_name      = issuer_name,
                    issuer_name_norm = normalize_name(issuer_name) or "",
                    cik              = cik,
                    lei              = parent_lei,
                    figi             = None,
                    match_level      = "parent_chain",
                    entity_level     = entity_level,
                    score_total      = 0.0,
                    supporting_evidence = {"chain_depth": chain_item["depth"]},
                )
                candidates.append(cand)
    return candidates[:10]

def generate_substr_candidates(
    entity: AwardeeEntity | ParentEntity,
    references: ReferenceHandles,
    config: ResolverConfig,
    entity_level: str = "awardee",
) -> list[IssuerCandidate]:
    """
    Substring match: find EDGAR entries whose stripped name is contained in the
    entity's stripped name (catches subsidiaries like 'Boeing Defense Space and Security' → BA).
    Mirrors old TickerResolverV4 Tier 4.
    """
    if not references.substr_index:
        return []
    candidates = []
    names_to_try = _entity_search_names(entity)[:2]
    seen_keys: set[str] = set()
    for name in names_to_try:
        norm     = normalize_name(name) or ""
        stripped = strip_legal_suffixes(norm)
        if not stripped:
            continue
        matches = lookup_substr_candidates(stripped, references.substr_index)
        for edgar_stripped, orig_name, entry, match_len in matches:
            key = entry.get("cik", "") or orig_name
            if key in seen_keys:
                continue
            seen_keys.add(key)
            cik    = entry.get("cik", "").zfill(10) if entry.get("cik") else None
            ticker = entry.get("ticker", "")
            cand   = IssuerCandidate(
                candidate_id     = str(uuid.uuid4())[:8],
                source           = "sec_exact",
                issuer_key       = f"cik:{cik}" if cik else f"name:{orig_name}",
                issuer_name      = orig_name,
                issuer_name_norm = normalize_name(orig_name) or "",
                cik              = cik,
                lei              = None,
                figi             = None,
                match_level      = "substring",
                entity_level     = entity_level,
                score_total      = 0.0,
                supporting_evidence = {
                    "match_len":      match_len,
                    "edgar_stripped": edgar_stripped,
                    "entity_stripped": stripped,
                },
            )
            candidates.append(cand)
    return candidates

def merge_and_deduplicate_candidates(
    sec_candidates: list[IssuerCandidate],
    gleif_candidates: list[IssuerCandidate],
    config: ResolverConfig,
) -> list[IssuerCandidate]:
    """Merge SEC and GLEIF candidates, deduplicating by CIK/LEI."""
    seen_keys: set[str] = set()
    merged: list[IssuerCandidate] = []
    for cand in sec_candidates + gleif_candidates:
        key = cand.issuer_key
        if key not in seen_keys:
            seen_keys.add(key)
            merged.append(cand)
    return merged

def prune_candidate_list(candidates: list[IssuerCandidate], config: ResolverConfig) -> list[IssuerCandidate]:
    """Remove candidates with scores too low to be useful."""
    min_score = config.thresholds.entity_resolve_min_score * 0.3  # conservative prune
    return [c for c in candidates if c.score_total >= min_score or c.score_total == 0.0]

# ── Scoring ───────────────────────────────────────────────────────────────────

def score_name_similarity(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    entity_norm    = entity.canonical_name_norm or ""
    entity_stripped = strip_legal_suffixes(entity_norm)
    cand_norm      = candidate.issuer_name_norm
    cand_stripped  = strip_legal_suffixes(cand_norm) if cand_norm else ""

    if entity_norm == cand_norm or entity_stripped == cand_stripped:
        return 50.0  # exact

    # Substring detection: applies to explicitly-tagged substring candidates AND to
    # sec_exact/fuzzy candidates where the EDGAR stripped name is embedded in entity name.
    # e.g. entity="LOCKHEED MARTIN SPACE SYSTEMS" found via sec_exact for "LOCKHEED MARTIN CORP"
    edgar_in_entity = bool(cand_stripped and entity_stripped and cand_stripped in entity_stripped)
    entity_in_edgar = bool(cand_stripped and entity_stripped and entity_stripped in cand_stripped)

    if edgar_in_entity or entity_in_edgar or candidate.match_level == "substring":
        edgar_s   = cand_stripped or ""
        entity_s  = entity_stripped or ""
        if edgar_s and edgar_s in entity_s:
            # Direction 1: EDGAR name is root/prefix of entity (parent-name match)
            entity_len = len(entity_s) or 1
            edgar_len  = len(edgar_s)
            frac = edgar_len / entity_len
            if frac >= 0.9:
                return 50.0
            if frac >= 0.6:
                return 46.0
            if frac >= 0.4:
                return 44.0
            return 42.0  # short parent-name minimum — above min_score threshold
        elif entity_s and entity_s in edgar_s:
            # Direction 2: entity name is contained in longer EDGAR name
            edgar_len = len(edgar_s) or 1
            coverage  = len(entity_s) / edgar_len
            if coverage >= 0.9:
                return 50.0
            if coverage >= 0.7:
                return 42.0
            if coverage >= 0.5:
                return 35.0
            return 20.0

    score = fuzz.token_sort_ratio(entity_stripped, cand_stripped)
    if score >= 95:
        return 45.0
    if score >= 85:
        return 30.0
    if score >= 75:
        return 20.0
    if score >= 70:
        return 12.0
    return max(0.0, score * 0.1)

def score_former_name_support(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    cik = candidate.cik
    if not cik:
        return 0.0
    issuer = config._sec_issuer_ref.get(cik, {}) if hasattr(config, "_sec_issuer_ref") else {}
    former_names = issuer.get("formerNames", [])
    entity_stripped = strip_legal_suffixes(entity.canonical_name_norm or "")
    for fn in former_names:
        fn_name = fn.get("name", "") if isinstance(fn, dict) else str(fn)
        fn_norm = normalize_name(fn_name) or ""
        fn_stripped = strip_legal_suffixes(fn_norm)
        if entity_stripped == fn_stripped:
            return 10.0
        if fuzz.token_sort_ratio(entity_stripped, fn_stripped) >= 85:
            return 7.0
    return 0.0

def score_domain_support(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    if not entity.domains:
        return 0.0
    cand_domains = candidate.supporting_evidence.get("domains", [])
    if not cand_domains:
        return 0.0
    shared = set(entity.domains) & set(cand_domains)
    return config.thresholds.domain_support_bonus if shared else 0.0

def score_address_support(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    entity_addresses = getattr(entity, "addresses", [])
    if not entity_addresses:
        return 0.0
    cand_addrs = candidate.supporting_evidence.get("addresses", [])
    if not cand_addrs:
        return 0.0
    best = max(
        address_similarity(ea, ca)
        for ea in entity_addresses
        for ca in cand_addrs
    )
    return config.thresholds.address_support_bonus * best

def score_country_support(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    entity_countries: set[str] = set()
    if isinstance(entity, AwardeeEntity):
        entity_countries = {a.get("country", "") for a in getattr(entity, "addresses", []) if a.get("country")}
    else:
        entity_countries = set(getattr(entity, "countries", []))
    cand_country = candidate.supporting_evidence.get("country", "")
    if cand_country and cand_country in entity_countries:
        return 5.0
    return 0.0

def score_parent_chain_support(entity, candidate: IssuerCandidate, config) -> float:
    if candidate.source == "gleif_parent_chain":
        depth = candidate.supporting_evidence.get("chain_depth", 0)
        return max(0.0, 10.0 - depth * 2.0)  # deeper = less credit
    return 0.0

def score_identifier_support(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    score = 0.0
    if isinstance(entity, AwardeeEntity) and entity.cage_code:
        cand_cage = candidate.supporting_evidence.get("cage_code")
        if cand_cage and cand_cage == entity.cage_code:
            score += 15.0
    if entity.canonical_name_norm:
        # Alias name match in candidate's evidence
        alias_names = candidate.supporting_evidence.get("alias_names", [])
        entity_stripped = strip_legal_suffixes(entity.canonical_name_norm)
        for alias in alias_names:
            alias_norm = normalize_name(alias) or ""
            if entity_stripped == strip_legal_suffixes(alias_norm):
                score += 8.0
                break
    return score

def score_conflicts(entity: AwardeeEntity | ParentEntity, candidate: IssuerCandidate, config) -> float:
    """Penalize when there is evidence conflict."""
    penalty = 0.0
    # Country conflict
    entity_countries: set[str] = set()
    if isinstance(entity, AwardeeEntity):
        entity_countries = {a.get("country", "") for a in getattr(entity, "addresses", []) if a.get("country")}
    else:
        entity_countries = set(getattr(entity, "countries", []))
    cand_country = candidate.supporting_evidence.get("country", "")
    if entity_countries and cand_country and cand_country not in entity_countries:
        # Both have country info and they differ
        if "USA" in entity_countries and cand_country not in ("USA", "US"):
            penalty += 10.0  # entity is US, candidate is foreign
    return -abs(penalty)

def build_score_breakdown(entity, candidate: IssuerCandidate, config) -> dict:
    return {
        "name_similarity":      score_name_similarity(entity, candidate, config),
        "former_name_support":  score_former_name_support(entity, candidate, config),
        "domain_support":       score_domain_support(entity, candidate, config),
        "address_support":      score_address_support(entity, candidate, config),
        "country_support":      score_country_support(entity, candidate, config),
        "parent_chain_support": score_parent_chain_support(entity, candidate, config),
        "identifier_support":   score_identifier_support(entity, candidate, config),
        "conflict_penalty":     score_conflicts(entity, candidate, config),
    }

def score_candidate(entity, candidate: IssuerCandidate, config) -> IssuerCandidate:
    """Compute total score and attach breakdown to candidate (returns updated copy)."""
    breakdown = build_score_breakdown(entity, candidate, config)
    total     = sum(breakdown.values())
    # Acronym-only penalty
    is_acronym_only = bool(
        entity.canonical_name_norm
        and len(entity.canonical_name_norm) <= 5
        and re.fullmatch(r"[A-Z0-9]+", entity.canonical_name_norm)
    )
    if is_acronym_only:
        total += config.thresholds.acronym_only_penalty
        breakdown["acronym_penalty"] = config.thresholds.acronym_only_penalty
    candidate.score_total      = max(0.0, total)
    candidate.score_components = breakdown
    return candidate

# ── SEC validation (live fallback) ────────────────────────────────────────────

def validate_candidate_vs_sec(candidate: IssuerCandidate, entity, config) -> tuple[bool, str]:
    """Fetch live SEC submission for the candidate CIK and verify name match."""
    if not candidate.cik or not config.source_policies.allow_sec_live_fallback:
        return False, "no_cik"
    meta = fetch_sec_entity_metadata(candidate.cik, config)
    if not meta:
        return False, "fetch_failed"
    sec_name_norm    = normalize_name(meta.get("name", "")) or ""
    sec_name_stripped = strip_legal_suffixes(sec_name_norm)
    entity_norm      = entity.canonical_name_norm or ""
    entity_stripped  = strip_legal_suffixes(entity_norm)
    if entity_norm == sec_name_norm or entity_stripped == sec_name_stripped:
        return True, "sec_exact"
    if fuzz.token_sort_ratio(entity_stripped, sec_name_stripped) >= 85:
        return True, "sec_fuzzy"
    for fn in meta.get("formerNames", []):
        fn_name    = fn.get("name", "") if isinstance(fn, dict) else str(fn)
        fn_stripped = strip_legal_suffixes(normalize_name(fn_name) or "")
        if entity_stripped == fn_stripped:
            return True, "former_name"
        if fuzz.token_sort_ratio(entity_stripped, fn_stripped) >= 85:
            return True, "former_name_fuzzy"
    # Check tickers exist
    if not meta.get("tickers"):
        return False, "no_tickers"
    return False, "name_mismatch"

# ── Decision ──────────────────────────────────────────────────────────────────

def choose_entity_resolution(
    candidates: list[IssuerCandidate],
    config: ResolverConfig,
) -> EntityResolutionDecision:
    """Pick the best candidate or return null decision."""
    if not candidates:
        return EntityResolutionDecision(
            entity_key="", entity_type="awardee",
            decision_status="no_match",
            matched_issuer_key=None, matched_issuer_name=None,
            matched_cik=None, matched_lei=None,
        )
    sorted_cands = sorted(candidates, key=lambda c: c.score_total, reverse=True)
    top     = sorted_cands[0]
    second  = sorted_cands[1] if len(sorted_cands) > 1 else None
    top_s   = top.score_total
    sec_s   = second.score_total if second else None
    gap     = (top_s - sec_s) if sec_s is not None else None

    is_null, null_reason = should_return_null_for_entity(sorted_cands, config)
    if is_null:
        return EntityResolutionDecision(
            entity_key="", entity_type=top.entity_level,
            decision_status="ambiguous" if null_reason == "ambiguous" else "no_match",
            matched_issuer_key=None, matched_issuer_name=None,
            matched_cik=None, matched_lei=None,
            top_score=top_s, second_score=sec_s, score_gap=gap,
            resolution_path=f"null:{null_reason}",
        )
    return EntityResolutionDecision(
        entity_key="", entity_type=top.entity_level,
        decision_status="resolved_issuer",
        matched_issuer_key=top.issuer_key,
        matched_issuer_name=top.issuer_name,
        matched_cik=top.cik,
        matched_lei=top.lei,
        top_score=top_s,
        second_score=sec_s,
        score_gap=gap,
        resolution_path=top.source,
        evidence_json=top.score_components,
    )

def is_candidate_gap_sufficient(top_score: float, second_score: float | None, config) -> bool:
    if second_score is None:
        return True
    return (top_score - second_score) >= config.thresholds.entity_resolve_gap_min

def should_return_null_for_entity(candidates: list[IssuerCandidate], config) -> tuple[bool, str | None]:
    if not candidates:
        return True, "no_match"
    top = candidates[0]
    if top.score_total < config.thresholds.entity_resolve_min_score:
        return True, "low_score"
    # Skip gap check when top candidate is high-confidence (exact, near-exact, or substring)
    # This prevents rejecting clean single-candidate matches due to fuzzy runner-ups
    if top.score_total >= 42.0 and top.match_level in ("exact", "fuzzy", "substring"):
        pass  # trust the top candidate
    elif len(candidates) > 1:
        second = candidates[1]
        if not is_candidate_gap_sufficient(top.score_total, second.score_total, config):
            return True, "ambiguous"
    # Conflict penalty alone should not force null unless it's very severe
    conflict = top.score_components.get("conflict_penalty", 0.0)
    if conflict <= config.thresholds.null_if_conflict_penalty:
        return True, "conflict"
    return False, None

# ── Parent-first resolution ───────────────────────────────────────────────────

def resolve_entity(
    entity: AwardeeEntity | ParentEntity,
    references: ReferenceHandles,
    config: ResolverConfig,
    entity_level: str = "awardee",
) -> EntityResolutionDecision:
    """Resolve one entity: known aliases → null classifier → candidates → score → decide."""

    # Known alias fast path
    alias_ticker = check_known_aliases(entity)
    if alias_ticker:
        return EntityResolutionDecision(
            entity_key=entity.entity_key,
            entity_type=entity_level,
            decision_status="resolved_issuer",
            matched_issuer_key=f"alias:{alias_ticker}",
            matched_issuer_name=entity.canonical_name or "",
            matched_cik=None,
            matched_lei=None,
            top_score=100.0,
            resolution_path="known_alias",
            evidence_json={"ticker": alias_ticker},
        )

    # Null classifier
    is_null, null_reason = classify_obvious_null_entity(entity, config)
    if is_null:
        return EntityResolutionDecision(
            entity_key=entity.entity_key,
            entity_type=entity_level,
            decision_status="private",
            matched_issuer_key=None, matched_issuer_name=None,
            matched_cik=None, matched_lei=None,
            resolution_path=f"null:{null_reason}",
        )

    # Candidate generation
    sec_cands    = generate_sec_candidates(entity, references, config, entity_level)
    substr_cands = generate_substr_candidates(entity, references, config, entity_level)
    gleif_cands  = generate_gleif_candidates(entity, references, config, entity_level)
    chain_cands  = generate_parent_chain_candidates(entity, references, config, entity_level)
    all_cands    = merge_and_deduplicate_candidates(
        sec_cands + substr_cands, gleif_cands + chain_cands, config
    )

    # Score all candidates
    scored = [score_candidate(entity, c, config) for c in all_cands]
    scored.sort(key=lambda c: c.score_total, reverse=True)

    # If no candidates exceed minimum, try SEC live fallback
    if (not scored or scored[0].score_total < config.thresholds.entity_resolve_min_score) \
            and config.source_policies.allow_sec_live_fallback:
        for cand in scored[:3]:
            ok, evidence_type = validate_candidate_vs_sec(cand, entity, config)
            if ok:
                cand.score_total     = max(cand.score_total, config.thresholds.entity_resolve_min_score + 5)
                cand.score_components["sec_live_validation"] = 5.0
                break
        scored.sort(key=lambda c: c.score_total, reverse=True)

    decision = choose_entity_resolution(scored, config)
    decision.entity_key = entity.entity_key
    return decision

def resolve_parent_first(
    awardee_entity: AwardeeEntity,
    parent_entity:  ParentEntity | None,
    references:     ReferenceHandles,
    config:         ResolverConfig,
) -> EntityResolutionDecision:
    """
    Try to resolve the parent first. Fall back to awardee if parent fails.
    Return the decision at the lowest justified public issuer level.
    """
    parent_decision  = None
    awardee_decision = None

    if parent_entity:
        parent_decision = resolve_entity(parent_entity, references, config, "parent")
        if parent_decision.decision_status == "resolved_issuer":
            # Check if awardee itself resolves to something different (lower entity)
            awardee_decision = resolve_entity(awardee_entity, references, config, "awardee")
            if awardee_decision.decision_status == "resolved_issuer":
                # Prefer the awardee if it resolves separately (lower in chain)
                if awardee_decision.matched_issuer_key != parent_decision.matched_issuer_key:
                    return awardee_decision
            return parent_decision

    # Parent didn't resolve — try awardee
    awardee_decision = resolve_entity(awardee_entity, references, config, "awardee")
    return awardee_decision

def should_fallback_to_awardee(parent_decision: EntityResolutionDecision, config) -> bool:
    return parent_decision.decision_status != "resolved_issuer"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _entity_search_names(entity: AwardeeEntity | ParentEntity) -> list[str]:
    """Return ordered list of names to try for an entity, deduplicated."""
    names = []
    seen: set[str] = set()
    for name in [entity.canonical_name] + list(entity.alias_names):
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names

def merge_entity_and_historical_decisions(
    entity_decision:   EntityResolutionDecision,
    historical_decision,
    security_decision,
    config: ResolverConfig,
) -> dict:
    """Merge three stage decisions into final resolution metadata dict."""
    import json as _json
    from resolver.models import ResolverStatus

    if entity_decision.decision_status == "private":
        status = ResolverStatus.NULL_PRIVATE.value
        ticker = None
        null_reason = entity_decision.resolution_path.replace("null:", "")
    elif entity_decision.decision_status in ("ambiguous", "no_match"):
        status = ResolverStatus.NULL_AMBIGUOUS_ENTITY.value
        ticker = None
        null_reason = entity_decision.resolution_path.replace("null:", "no_match")
    elif historical_decision and historical_decision.status != "resolved_symbol":
        status = ResolverStatus.NULL_HISTORICAL_UNAVAILABLE.value
        ticker = None
        null_reason = historical_decision.status
    elif security_decision and security_decision.status != "resolved_security":
        status = ResolverStatus.NULL_NO_US_TRADABLE_SECURITY.value
        ticker = None
        null_reason = "no_us_tradable_security"
    else:
        status = ResolverStatus.RESOLVED.value
        ticker = security_decision.selected_ticker if security_decision else None
        null_reason = None
        if entity_decision.resolution_path == "known_alias":
            ticker = entity_decision.evidence_json.get("ticker")
            status = ResolverStatus.RESOLVED.value

    return {
        "resolver_status":               status,
        "resolver_ticker":               ticker,
        "resolver_exchange":             security_decision.selected_exchange if security_decision else None,
        "resolver_security_type":        security_decision.selected_security_type if security_decision else None,
        "resolver_is_adr":               security_decision.is_adr if security_decision else None,
        "resolver_confidence":           _map_confidence(entity_decision.top_score),
        "resolver_resolution_path":      entity_decision.resolution_path,
        "resolver_entity_level_used":    entity_decision.entity_type,
        "resolver_entity_match_source":  entity_decision.resolution_path,
        "resolver_top_candidate_score":  entity_decision.top_score,
        "resolver_second_candidate_score": entity_decision.second_score,
        "resolver_candidate_gap":        entity_decision.score_gap,
        "resolver_null_reason":          null_reason,
        "resolver_matched_issuer_name":  entity_decision.matched_issuer_name,
        "resolver_matched_cik":          entity_decision.matched_cik,
        "resolver_matched_lei":          entity_decision.matched_lei,
        "resolver_score_breakdown_json": _json.dumps(entity_decision.evidence_json),
    }

def _map_confidence(score: float) -> str:
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium_high"
    if score >= 45:
        return "medium"
    if score >= 30:
        return "low_medium"
    return "low"
