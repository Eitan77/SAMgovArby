"""resolver/reference.py — SEC master, GLEIF master, ISIN-LEI, security cache, reference handles."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path

from resolver.models import ReferenceHandles, RefreshReport
from resolver.normalize import normalize_name, strip_legal_suffixes, normalize_lei

log = logging.getLogger(__name__)

# ── SEC issuer master ─────────────────────────────────────────────────────────

def create_sec_issuer_master(edgar_map: dict, config) -> dict:
    """
    Build a CIK-keyed issuer master from the raw edgar_map {name: {ticker, cik}}.
    Augments with formerly-cached SEC metadata where available.
    """
    # edgar_map already gives us name→{ticker,cik}
    # Build reverse: cik → {name, ticker, cik}
    cik_master: dict = {}
    for name, entry in edgar_map.items():
        cik    = entry.get("cik", "")
        ticker = entry.get("ticker", "")
        if not cik:
            continue
        cik = cik.zfill(10)
        if cik not in cik_master:
            cik_master[cik] = {
                "cik":        cik,
                "name":       name,
                "tickers":    [ticker] if ticker else [],
                "exchanges":  [],
                "formerNames":[],
                "entityType": "",
                "sic":        "",
            }
        else:
            if ticker and ticker not in cik_master[cik]["tickers"]:
                cik_master[cik]["tickers"].append(ticker)
    return cik_master

def create_sec_alias_table(sec_issuer_master: dict, edgar_map: dict) -> dict:
    """
    Build a name_norm → [cik, ...] lookup from sec_issuer_master + edgar_map.
    Includes both current names and former names where available.
    """
    alias_table: dict[str, list[str]] = {}

    def _add(name: str, cik: str):
        norm = normalize_name(name)
        if not norm:
            return
        alias_table.setdefault(norm, [])
        if cik not in alias_table[norm]:
            alias_table[norm].append(cik)
        stripped = strip_legal_suffixes(norm)
        if stripped and stripped != norm:
            alias_table.setdefault(stripped, [])
            if cik not in alias_table[stripped]:
                alias_table[stripped].append(cik)

    # From edgar_map (all company names → cik)
    for name, entry in edgar_map.items():
        cik = entry.get("cik", "")
        if cik:
            _add(name, cik.zfill(10))

    # From issuer master former names
    for cik, issuer in sec_issuer_master.items():
        for fn in issuer.get("formerNames", []):
            fname = fn.get("name", "") if isinstance(fn, dict) else str(fn)
            if fname:
                _add(fname, cik)

    return alias_table

def create_sec_filing_locator(sec_issuer_master: dict) -> dict:
    """Build CIK → filing list from issuer master (filings embedded in metadata)."""
    locator: dict = {}
    for cik, issuer in sec_issuer_master.items():
        filings = issuer.get("filings", {})
        if filings:
            locator[cik] = filings
    return locator

def lookup_sec_candidates_by_name(name_norm: str, alias_table: dict, limit: int = 50) -> list[str]:
    """Return list of CIKs that match the normalized name."""
    direct  = alias_table.get(name_norm, [])
    stripped = strip_legal_suffixes(name_norm)
    extra   = alias_table.get(stripped, []) if stripped != name_norm else []
    seen: set[str] = set()
    result  = []
    for cik in direct + extra:
        if cik not in seen:
            seen.add(cik)
            result.append(cik)
        if len(result) >= limit:
            break
    return result

# ── GLEIF master ──────────────────────────────────────────────────────────────

def create_gleif_entity_master(raw_gleif_data: dict, config) -> dict:
    """Wrap raw GLEIF map (lei→entity) for use in reference handles."""
    return raw_gleif_data  # already in the right shape from ingest

def create_gleif_alias_table(gleif_master: dict) -> dict:
    """Build name_norm → [lei, ...] lookup from GLEIF entity master."""
    alias_table: dict[str, list[str]] = {}
    for lei, entity in gleif_master.items():
        name = entity.get("name", "")
        if not name:
            continue
        norm = normalize_name(name)
        if norm:
            alias_table.setdefault(norm, [])
            if lei not in alias_table[norm]:
                alias_table[norm].append(lei)
            stripped = strip_legal_suffixes(norm)
            if stripped and stripped != norm:
                alias_table.setdefault(stripped, [])
                if lei not in alias_table[stripped]:
                    alias_table[stripped].append(lei)
    return alias_table

def create_gleif_relationship_table(raw_gleif_data: dict) -> dict:
    """Build lei → {direct_parent_lei, ultimate_parent_lei} from GLEIF data."""
    rels: dict = {}
    for lei, entity in raw_gleif_data.items():
        dp = entity.get("direct_parent_lei")
        up = entity.get("ultimate_parent_lei")
        if dp or up:
            rels[lei] = {"direct_parent_lei": dp, "ultimate_parent_lei": up}
    return rels

def build_substr_index(edgar_map: dict) -> list[tuple[str, str, dict]]:
    """
    Build a list of (stripped_edgar_name, original_name, entry) for substring matching.
    Mirrors the old TickerResolverV4._substr_candidates list.
    Minimum 4 chars to avoid noise (catches SAIC, CACI, AECOM).
    """
    index = []
    seen: set[str] = set()
    for name, entry in edgar_map.items():
        stripped = strip_legal_suffixes(normalize_name(name) or "")
        if stripped and len(stripped) >= 4 and stripped not in seen:
            seen.add(stripped)
            index.append((stripped, name, entry))
    return index

def lookup_substr_candidates(
    entity_stripped: str,
    substr_index: list[tuple[str, str, dict]],
    min_len: int = 4,
) -> list[tuple[str, str, dict, int]]:
    """
    Find EDGAR entries where the stripped EDGAR name is contained in the entity name
    or vice versa (subsidiary detection). Returns list of (edgar_stripped, name, entry, match_len).
    """
    import re
    results = []
    for edgar_stripped, orig_name, entry in substr_index:
        match_len = 0
        if edgar_stripped in entity_stripped:
            match_len = len(edgar_stripped)
        elif entity_stripped in edgar_stripped:
            match_len = len(entity_stripped)
        if match_len < min_len:
            continue
        # Coverage check:
        # Case 1 — edgar_stripped ⊆ entity_stripped (parent-name match, e.g. "BOEING" in "BOEING DEFENSE SPACE"):
        #   No coverage threshold — any match of edgar name inside entity name is intentional.
        # Case 2 — entity_stripped ⊆ edgar_stripped (entity name inside longer EDGAR name):
        #   Require entity covers >= 50% of edgar to avoid short-fragment false positives.
        if entity_stripped in edgar_stripped and edgar_stripped not in entity_stripped:
            if match_len / len(edgar_stripped) < 0.5:
                continue
        # Word-boundary check for short matches to avoid false positives
        if match_len < 7:
            if edgar_stripped in entity_stripped:
                # e.g. "SAIC" in "SAIC DEFENSE" — check edgar_stripped has word boundaries in entity
                pattern = r"(?<![A-Z0-9])" + re.escape(edgar_stripped) + r"(?![A-Z0-9])"
                if not re.search(pattern, entity_stripped):
                    continue
            elif entity_stripped in edgar_stripped:
                # e.g. "SAIC" in "MOSAIC" — check entity_stripped has word boundaries in edgar
                pattern = r"(?<![A-Z0-9])" + re.escape(entity_stripped) + r"(?![A-Z0-9])"
                if not re.search(pattern, edgar_stripped):
                    continue
        results.append((edgar_stripped, orig_name, entry, match_len))
    # Best match = longest match_len first
    results.sort(key=lambda x: x[3], reverse=True)
    return results[:5]

def lookup_gleif_candidates(
    name_norm: str, gleif_alias_table: dict,
    country: str | None = None, limit: int = 50
) -> list[str]:
    """Return list of LEIs matching the normalized name."""
    direct   = gleif_alias_table.get(name_norm, [])
    stripped = strip_legal_suffixes(name_norm)
    extra    = gleif_alias_table.get(stripped, []) if stripped != name_norm else []
    seen: set[str] = set()
    result   = []
    for lei in direct + extra:
        if lei not in seen:
            seen.add(lei)
            result.append(lei)
        if len(result) >= limit:
            break
    return result

def traverse_parent_chain(lei: str, gleif_relationships: dict, config, max_depth: int = 5) -> list[dict]:
    """Walk GLEIF parent chain from lei. Returns [{lei, rel_type}, ...]."""
    chain    = []
    visited  = set()
    current  = lei
    depth    = 0
    while current and depth < max_depth:
        if current in visited:
            break
        visited.add(current)
        rel = gleif_relationships.get(current)
        if not rel:
            break
        parent_lei = rel.get("direct_parent_lei")
        if not parent_lei or parent_lei == current:
            break
        chain.append({"lei": parent_lei, "rel_type": "direct_parent", "depth": depth + 1})
        current = parent_lei
        depth  += 1
    return chain

# ── ISIN-LEI master ───────────────────────────────────────────────────────────

def refresh_isin_lei_data(config) -> RefreshReport:
    """Download GLEIF ISIN-to-LEI mapping (optional high-confidence bridge)."""
    from resolver.persistence import record_refresh_event, get_latest_refresh_status
    started = datetime.utcnow()
    source  = "gleif_isin"
    last    = get_latest_refresh_status(source, config)
    if last and last["status"] == "ok":
        age_days = (datetime.utcnow() - datetime.fromisoformat(last["ended_at"])).days
        if age_days < 7:
            return RefreshReport(source, started, datetime.utcnow(), "skipped")
    try:
        import requests
        # GLEIF publishes ISIN-LEI mapping at a known endpoint
        url  = "https://mapping.gleif.org/api/v2/isin-lei/isin-lei.zip"
        log.info("ISIN-LEI mapping download not yet implemented (large file); using empty map")
        # For now, emit empty file so the system works without it
        out  = Path(config.paths.curated_dir) / "isin_lei_map.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        if not out.exists():
            with open(out, "w") as f:
                json.dump({}, f)
        ended = datetime.utcnow()
        record_refresh_event(source, started, ended, "skipped", {"reason": "not_implemented"}, config)
        return RefreshReport(source, started, ended, "skipped")
    except Exception as e:
        ended = datetime.utcnow()
        return RefreshReport(source, started, ended, "failed", error=str(e))

def load_isin_lei_map(config) -> dict:
    path = Path(config.paths.curated_dir) / "isin_lei_map.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)

def lookup_isins_for_lei(lei: str, isin_lei_map: dict) -> list[str]:
    # Build reverse map on the fly (map is isin→lei)
    return [isin for isin, l in isin_lei_map.items() if l == lei]

def lookup_lei_for_isin(isin: str, isin_lei_map: dict) -> str | None:
    return isin_lei_map.get(isin)

# ── Security cache ────────────────────────────────────────────────────────────

def upsert_security_rows(rows: list[dict], security_cache: dict) -> None:
    """Add/update security metadata in the in-memory security cache."""
    for row in rows:
        key = row.get("ticker") or row.get("figi") or row.get("isin")
        if key:
            security_cache[key] = row

def lookup_security_by_isin(isin: str, security_cache: dict) -> list[dict]:
    return [v for v in security_cache.values() if v.get("isin") == isin]

def lookup_security_by_figi(figi: str, security_cache: dict) -> dict | None:
    return security_cache.get(figi) or next(
        (v for v in security_cache.values() if v.get("figi") == figi), None
    )

def lookup_us_tradable_candidates(identifier_bundle: dict, security_cache: dict, config) -> list[dict]:
    """Find US-tradable securities for a given {ticker, figi, isin, cik} bundle."""
    from resolver.models import US_EXCHANGE_CODES_ALLOWED, DISALLOWED_SECURITY_TYPES
    results = []
    ticker = identifier_bundle.get("ticker")
    if ticker and ticker in security_cache:
        results.append(security_cache[ticker])
    figi = identifier_bundle.get("figi")
    if figi:
        r = lookup_security_by_figi(figi, security_cache)
        if r and r not in results:
            results.append(r)
    # Filter to US-tradable
    filtered = []
    for sec in results:
        exch     = sec.get("exchange", "")
        sec_type = sec.get("security_type", "")
        if exch in US_EXCHANGE_CODES_ALLOWED and sec_type not in DISALLOWED_SECURITY_TYPES:
            filtered.append(sec)
    return filtered

# ── Reference handle builder ──────────────────────────────────────────────────

def build_reference_handles(config) -> ReferenceHandles:
    """
    Load all curated reference data into memory as a ReferenceHandles object.
    This is the main entry point used by orchestration.
    """
    from resolver.ingest import load_sec_submissions_raw, load_gleif_master

    log.info("Building reference handles...")

    edgar_map = load_sec_submissions_raw(config)
    if not edgar_map:
        log.warning("SEC master not found — run refresh_reference_data('sec') first")

    sec_issuer_master = create_sec_issuer_master(edgar_map, config)
    sec_alias_table   = create_sec_alias_table(sec_issuer_master, edgar_map)
    sec_filing_locator = create_sec_filing_locator(sec_issuer_master)

    gleif_raw       = load_gleif_master(config)
    gleif_master    = create_gleif_entity_master(gleif_raw, config)
    gleif_alias     = create_gleif_alias_table(gleif_master)
    gleif_rels      = create_gleif_relationship_table(gleif_raw)

    isin_lei_map    = load_isin_lei_map(config)
    security_cache: dict = {}

    # Derive reference version from file mtimes
    sec_path = Path(config.paths.curated_dir) / "sec_master.json"
    ref_ver  = sec_path.stat().st_mtime_ns if sec_path.exists() else 0
    ref_ver  = str(ref_ver)[-8:]  # last 8 digits of ns timestamp

    substr_index = build_substr_index(edgar_map)

    handles = ReferenceHandles(
        edgar_map           = edgar_map,
        sec_issuer_master   = sec_issuer_master,
        sec_alias_table     = sec_alias_table,
        gleif_entity_master = gleif_master,
        gleif_alias_table   = gleif_alias,
        gleif_relationships = gleif_rels,
        isin_lei_map        = isin_lei_map,
        security_cache      = security_cache,
        substr_index        = substr_index,
        reference_version   = ref_ver,
    )
    log.info(
        f"Reference handles built: {len(edgar_map):,} SEC issuers, "
        f"{len(gleif_master):,} GLEIF entities, "
        f"{len(isin_lei_map):,} ISIN-LEI pairs"
    )
    return handles

def build_all_reference_tables(config) -> dict:
    """Refresh all sources then build handles. Returns handles dict."""
    from resolver.ingest import refresh_sec_submissions, refresh_gleif_data
    sec_report   = refresh_sec_submissions(config)
    gleif_report = refresh_gleif_data(config)
    isin_report  = refresh_isin_lei_data(config)
    handles      = build_reference_handles(config)
    return {
        "handles": handles,
        "reports": [sec_report, gleif_report, isin_report],
    }

def validate_reference_integrity(config) -> dict:
    """Check that required reference files exist and have content."""
    results = {}
    for name, path in [
        ("sec_master", Path(config.paths.curated_dir) / "sec_master.json"),
        ("gleif_master", Path(config.paths.curated_dir) / "gleif_master.json"),
        ("isin_lei_map", Path(config.paths.curated_dir) / "isin_lei_map.json"),
    ]:
        results[name] = path.exists() and path.stat().st_size > 10
    return results
