"""TickerResolverV4 — multi-tier resolver using SAM.gov ContractRecord fields.

Resolution pipeline (stops at first hit):
  Tier 0: Hard rejects (non-public flags, foreign country, name regex)
  Tier 1: CAGE → GLEIF → LEI → OpenFIGI (direct identifier lookup, highest accuracy)
           Fallback: GLEIF name search → LEI → OpenFIGI
  Tier 2: Multi-name EDGAR exact match (4 names: legal, contractor, dba, parent)
  Tier 3: Multi-name EDGAR fuzzy match (4 names, threshold 70/75)
  Tier 4: Substring match (catch subsidiaries)
  Tier 5: Sole-source tag (num_offers == "1" or not-competed → tag for scorer)

Improvements over V3:
  - Accepts ContractRecord (rich data) instead of a bare name string
  - Four name attempts per tier (legal > contractor > dba > parent)
  - Business-type flags enable zero-API non-public detection
  - Cache key uses CAGE/UEI for stable identity across name variations
  - Separate cache file (.ticker_cache_v4.json) — no V3 collision
  - Standalone: no imports from ticker_resolver_v3
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime

import requests
import yfinance as yf
from rapidfuzz import fuzz, process

from cage_resolver import CageResolver
from lei_resolver import LeiResolver
from sam_entity_client import SamEntityClient
from sam_gov_reader import ContractRecord
from config import EDGAR_RATE_LIMIT, EDGAR_USER_AGENT, user_cache_dir

log = logging.getLogger(__name__)

_MCAP_CACHE_WRITE_BATCH = 50

# GLEIF circuit breaker — after this many consecutive timeouts, skip GLEIF for
# the rest of the process session (avoids 10s hangs per entity during bulk builds)
_GLEIF_TIMEOUT_THRESHOLD = 3
_gleif_consecutive_timeouts = 0
_gleif_disabled = False

# ── Non-public entity patterns ────────────────────────────────────────────────
_NON_PUBLIC_PATTERNS = [
    r"\bUNIVERSIT",
    r"\bREGENTS\b",
    r"\bTRUSTEES\b",
    r"\bBOARD OF\b",
    r"\bNATIONAL LABORATOR",
    r"\bDEPARTMENT OF\b",
    r"\bBUREAU OF\b",
    r"\bFOUNDATION\b",
    r"\bINSTITUTE OF\b",
    r"\bAUTHORIT[YI]",
    r"\bTRIBAL\b",
    r"\bCOUNTY OF\b",
    r"\bCITY OF\b",
    r"\bSTATE OF\b",
    r"\bCOMMISSION\b",
    r"\bGOVERNMENT\b",
    r"\bMUNICIPAL",
    r"\bCOOPERATIVE\b",
    r"\bASSOCIATION OF\b",
    r"\bCONSORTIUM\b",
    r"\bJOINT VENTURE\b",
    r"\b[A-Z]+ JV\b",
    r"\bAJV\b",
    r"\bBATTELLE\b",
    r"\bSANDIA\b",
    r"\bBROOKHAVEN\b",
    r"\bFERMILAB\b",
]
_NON_PUBLIC_RE = [re.compile(p, re.IGNORECASE) for p in _NON_PUBLIC_PATTERNS]

# ── Suffixes to strip ─────────────────────────────────────────────────────────
_SUFFIX_WORDS = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "LLC", "LLP",
    "LTD", "LIMITED", "CO", "COMPANY", "LP", "HOLDINGS",
    "GROUP", "TECHNOLOGIES", "SOLUTIONS", "SYSTEMS", "SERVICES",
    "ENTERPRISES", "GLOBAL", "USA", "US", "DBA",
    # State of incorporation suffixes from SEC EDGAR names
    "DE", "MD", "NV", "NY", "VA", "CA", "TX", "FL", "PA", "OH",
    "WA", "GA", "MA", "IL", "NJ", "CT", "AZ", "CO", "MN",
}

# ── Extent Competed codes indicating no competition ───────────────────────────
_NOT_COMPETED_CODES = {"B", "C", "G", "CDO", "URG", "SP2"}

# ── Known company aliases: renames + acquisitions not in EDGAR map ────────────
# Keys are _strip_suffixes(_normalize(name)) OR _normalize(name) for ambiguous cases.
# Check both forms at resolution time.
KNOWN_ALIASES: dict[str, str] = {
    # Raytheon Technologies (RTX) predecessors
    "RAYTHEON": "RTX",
    "RAYTHEON BBN": "RTX",
    "RAYTHEON INTELLIGENCE AND SPACE": "RTX",
    "RAYTHEON MISSILES AND DEFENSE": "RTX",
    "UNITED TECHNOLOGIES": "RTX",
    # L3Harris (LHX) predecessors
    "HARRIS CORPORATION": "LHX",   # normalized (CORPORATION stripped → HARRIS, too ambiguous)
    "L3 COMMUNICATIONS": "LHX",
    "L3HARRIS": "LHX",
    # SAIC acquisitions
    "ENGILITY": "SAIC",
    # Vectrus → V2X
    "VECTRUS": "V2X",
}

# ── SEC EDGAR endpoints ───────────────────────────────────────────────────────
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_HEADERS = {"User-Agent": EDGAR_USER_AGENT, "Accept": "application/json"}

_EDGAR_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
_EDGAR_TICKERS_FALLBACK_URL = "https://www.sec.gov/files/company_tickers.json"

# Exchange tiers for common-stock preference (lower = more preferred)
_EXCHANGE_RANK = {"Nasdaq": 0, "NYSE": 0, "NYSEArca": 1, "NYSEAmerican": 1}
_OTC_EXCHANGE = "OTC"

_edgar_last = 0.0


def _edgar_throttle():
    global _edgar_last
    elapsed = time.time() - _edgar_last
    if elapsed < EDGAR_RATE_LIMIT:
        time.sleep(EDGAR_RATE_LIMIT - elapsed)
    _edgar_last = time.time()


# ── Text normalization ────────────────────────────────────────────────────────

def _normalize(name: str) -> str:
    upper = name.strip().upper()
    upper = upper.replace("&", "AND")
    upper = re.sub(r'[^A-Z0-9 ]', '', upper)
    return re.sub(r' +', ' ', upper).strip()


def _strip_suffixes(name: str) -> str:
    words = name.split()
    while words and words[-1] in _SUFFIX_WORDS:
        words.pop()
    result = " ".join(words)
    return result if result else name


# ── SEC Submissions validation ────────────────────────────────────────────────

def _fetch_submissions_metadata(cik: str) -> dict | None:
    if not cik:
        return None
    _edgar_throttle()
    try:
        url = EDGAR_SUBMISSIONS_URL.format(cik=str(cik).zfill(10))
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return {
            "name": data.get("name", ""),
            "formerNames": data.get("formerNames", []),
            "tickers": data.get("tickers", []),
            "exchanges": data.get("exchanges", []),
            "entityType": data.get("entityType", ""),
            "sic": data.get("sic", ""),
            "sicDescription": data.get("sicDescription", ""),
        }
    except Exception as e:
        log.debug(f"Submissions fetch failed for CIK {cik}: {e}")
        return None


def _validate_candidate(candidate_cik: str, awardee_norm: str, awardee_stripped: str) -> tuple[bool, str, str]:
    """Validate a candidate CIK against SEC submissions.

    Returns (valid, confidence, evidence_type).
    """
    meta = _fetch_submissions_metadata(candidate_cik)
    if not meta:
        return False, "none", "validation_failed"

    sec_name_norm = _normalize(meta["name"])
    sec_name_stripped = _strip_suffixes(sec_name_norm)

    if awardee_norm == sec_name_norm or awardee_stripped == sec_name_stripped:
        return True, "high", "exact_sec_name"

    score = fuzz.token_sort_ratio(awardee_stripped, sec_name_stripped)
    if score >= 90:
        return True, "high", "fuzzy_sec_name"

    for fn in meta.get("formerNames", []):
        fn_norm = _normalize(fn.get("name", ""))
        fn_stripped = _strip_suffixes(fn_norm)
        if awardee_norm == fn_norm or awardee_stripped == fn_stripped:
            return True, "medium_high", "former_name_exact"
        score = fuzz.token_sort_ratio(awardee_stripped, fn_stripped)
        if score >= 85:
            return True, "medium_high", "former_name_fuzzy"

    if not meta.get("tickers"):
        return False, "none", "no_tickers_on_file"

    return False, "low", "name_mismatch"


# ── EDGAR map loader ──────────────────────────────────────────────────────────

def _load_edgar_map_default() -> dict:
    """Load the EDGAR company→ticker map, using a persistent user-level cache.

    Primary source: SEC company_tickers_exchange.json (has exchange field for
    reliable common-stock preference over preferred/warrants).
    Fallback: SEC company_tickers.json (no exchange field, character filtering).
    Cache lives in ~/.cache/samgovarby/ so it survives git clean / code resets.
    """
    edgar_map_file = os.path.join(user_cache_dir(), "edgar_tickers.json")

    if os.path.exists(edgar_map_file):
        age_days = (time.time() - os.path.getmtime(edgar_map_file)) / 86400
        if age_days < 7:
            try:
                with open(edgar_map_file) as f:
                    data = json.load(f)
                # Migrate legacy format (name → ticker string) to dict format
                migrated = {}
                for name, val in data.items():
                    if isinstance(val, str):
                        migrated[name] = {"ticker": val, "cik": ""}
                    else:
                        migrated[name] = val
                if migrated:
                    return migrated
            except Exception as e:
                log.warning(f"Could not load EDGAR map from cache: {e}")

    # Primary: SEC company_tickers_exchange.json
    # Has exchange field — prefer NYSE/Nasdaq over OTC to filter preferred stock.
    log.info("Downloading EDGAR company tickers (with exchange) from SEC...")
    try:
        resp = requests.get(_EDGAR_TICKERS_EXCHANGE_URL, headers=EDGAR_HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        fields = raw.get("fields", [])  # ["cik", "name", "ticker", "exchange"]
        rows = raw.get("data", [])
        idx = {f: i for i, f in enumerate(fields)}

        # Per name, track candidates and pick best: prefer listed exchanges > OTC,
        # then no special chars, then shortest ticker.
        candidates: dict[str, list] = {}  # name_upper → list of (exchange_rank, is_special, ticker_len, ticker, cik)
        for row in rows:
            cik = str(row[idx["cik"]])
            name = row[idx["name"]].strip().upper()
            ticker = row[idx["ticker"]].strip().upper()
            exchange = row[idx.get("exchange", -1)] if "exchange" in idx else ""
            if not name or not ticker:
                continue
            is_special = any(c in ticker for c in "-/+")
            exrank = _EXCHANGE_RANK.get(exchange, 2) if exchange != _OTC_EXCHANGE else 3
            entry = (exrank, int(is_special), len(ticker), ticker, cik)
            candidates.setdefault(name, []).append(entry)

        edgar_map: dict = {}
        for name, entries in candidates.items():
            entries.sort()  # sorts by (exrank, is_special, len, ticker, cik) — lowest wins
            best = entries[0]
            edgar_map[name] = {"ticker": best[3], "cik": best[4]}

        with open(edgar_map_file, "w") as f:
            json.dump(edgar_map, f)
        log.info(f"EDGAR map (exchange-filtered) downloaded: {len(edgar_map):,} companies → {edgar_map_file}")
        return edgar_map
    except Exception as e:
        log.warning(f"company_tickers_exchange.json failed ({e}), falling back to basic SEC download")

    # Fallback: SEC company_tickers.json (no exchange field — use character filtering)
    log.info("Downloading EDGAR company tickers (basic) from SEC...")
    try:
        resp = requests.get(_EDGAR_TICKERS_FALLBACK_URL, headers=EDGAR_HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        edgar_map = {}
        for entry in raw.values():
            name = entry.get("title", "").strip().upper()
            ticker = entry.get("ticker", "").strip().upper()
            cik = str(entry.get("cik_str", ""))
            if not name or not ticker:
                continue
            is_special = any(c in ticker for c in "-/+")
            if name in edgar_map:
                existing_is_special = any(c in edgar_map[name]["ticker"] for c in "-/+")
                if not existing_is_special:
                    continue
                if is_special:
                    continue
            edgar_map[name] = {"ticker": ticker, "cik": cik}
        with open(edgar_map_file, "w") as f:
            json.dump(edgar_map, f)
        log.info(f"EDGAR map (basic) downloaded: {len(edgar_map):,} companies → {edgar_map_file}")
        return edgar_map
    except Exception as e:
        log.error(f"Failed to download EDGAR tickers: {e}")
        return {}


# ── Main resolver ─────────────────────────────────────────────────────────────

class TickerResolverV4:
    """5-tier resolver operating on ContractRecord from sam_gov_reader."""

    def __init__(self, edgar_map: dict | None = None,
                 cache_path: str = ".ticker_cache_v4.json",
                 mcap_cache_path: str | None = None,
                 gleif_name_search: bool = False):
        if edgar_map is None:
            edgar_map = _load_edgar_map_default()
        self.edgar_map = edgar_map
        self.cache_path = cache_path
        # mcap cache is universal (same data across datasets) — store in user cache dir
        if mcap_cache_path is None:
            mcap_cache_path = os.path.join(user_cache_dir(), "mcap_cache.json")
        self.mcap_cache_path = mcap_cache_path
        self.cache: dict = {}
        self.mcap_cache: dict = {}
        self._mcap_unsaved = 0
        self.cage_resolver = CageResolver()
        self.lei_resolver = LeiResolver()
        self.sam_entity_client = SamEntityClient()
        self.gleif_name_search = gleif_name_search  # disabled by default (too slow for bulk)

        if cache_path != ":memory:":
            self._load_cache()
            self._load_mcap_cache()

        # Pre-build EDGAR lookup indices
        self._stripped_map: dict[str, tuple[str, dict]] = {}
        self._edgar_names: list[str] = list(edgar_map.keys())
        self._substr_candidates: list[tuple[str, str, dict]] = []
        for ename, entry in edgar_map.items():
            s = _strip_suffixes(_normalize(ename))
            if s and s not in self._stripped_map:
                self._stripped_map[s] = (ename, entry)
            if s and len(s) >= 4:  # was >= 2 words; now catches AECOM, SAIC, CACI
                self._substr_candidates.append((s, ename, entry))

    # ── Cache I/O ─────────────────────────────────────────────────────────────

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path) as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {}

    def _load_mcap_cache(self):
        if os.path.exists(self.mcap_cache_path):
            try:
                with open(self.mcap_cache_path) as f:
                    self.mcap_cache = json.load(f)
            except Exception:
                self.mcap_cache = {}

    def save_cache(self):
        if self.cache_path == ":memory:":
            return
        with open(self.cache_path, "w") as f:
            json.dump(self.cache, f, indent=2)
        if self._mcap_unsaved > 0:
            self._flush_mcap_cache()

    def _flush_mcap_cache(self):
        if self.mcap_cache_path == ":memory:":
            return
        with open(self.mcap_cache_path, "w") as f:
            json.dump(self.mcap_cache, f, indent=2)
        self._mcap_unsaved = 0

    # ── Public API ────────────────────────────────────────────────────────────

    def resolve(self, record: ContractRecord) -> dict:
        """Resolve a ContractRecord → ticker result dict.

        Cache key prefers stable identifiers (CAGE, UEI) over mutable names.
        """
        cache_key = (record.cage_code or record.uei or
                     record.legal_business_name or record.contractor_name)
        if cache_key and cache_key in self.cache:
            return self.cache[cache_key]

        result = self._resolve(record)

        if cache_key:
            self.cache[cache_key] = result
        return result

    # ── Resolution pipeline ───────────────────────────────────────────────────

    def _resolve(self, record: ContractRecord) -> dict:
        primary_name = record.contractor_name or record.legal_business_name

        # Tier 0: hard rejects
        if self._is_non_public(record):
            return self._make_result(primary_name, _normalize(primary_name),
                                     None, None, "none", "unresolved", "non_public_entity")

        # Tier 0.5: known aliases for renamed/acquired companies
        for name in [record.legal_business_name, record.contractor_name,
                     record.dba_name, record.parent_name]:
            if not name.strip():
                continue
            norm = _normalize(name)
            stripped = _strip_suffixes(norm)
            for key in (stripped, norm):
                if key in KNOWN_ALIASES:
                    ticker = KNOWN_ALIASES[key]
                    mc = self._get_market_cap(ticker)
                    return self._make_result(primary_name, _normalize(primary_name),
                                             ticker, "", "high", "known_alias", None, mc)

        # Tier 1: CAGE → GLEIF → LEI → OpenFIGI (+ SAM.gov Entity API fallback)
        if record.cage_code:
            r = self._resolve_via_cage(record)
            if r.get("resolved_ticker"):
                return r

        # Names to try in Tiers 2–4: parent first (most likely SEC-registered entity),
        # then legal, contractor, dba. Deduplicate preserving order.
        _seen: set[str] = set()
        names = []
        for n in [record.parent_name, record.legal_business_name,
                  record.contractor_name, record.dba_name]:
            if n.strip() and n not in _seen:
                _seen.add(n)
                names.append(n)

        # Tier 2: multi-name exact match
        for name in names:
            r = self._exact_match(name)
            if r:
                return r

        # Tier 3: multi-name fuzzy match
        for name in names:
            r = self._fuzzy_match(name)
            if r:
                return r

        # Tier 4: substring match (subsidiary catch)
        for name in [record.contractor_name, record.legal_business_name]:
            if name.strip():
                r = self._substring_match(name)
                if r:
                    return r

        # Tier 4.5: GLEIF name search — opt-in only (slow: ~1-3s/entity even on cache miss)
        # Disabled by default for bulk builds. Enable via gleif_name_search=True for
        # small targeted runs (e.g. re-resolving a curated set of unresolved entities).
        if self.gleif_name_search and not _gleif_disabled:
            for name in names[:2]:
                norm = _normalize(name)
                r = self._gleif_name_to_ticker(
                    _strip_suffixes(norm), name, norm, "gleif_name"
                )
                if r:
                    return r

        # Tier 5: sole-source tag
        rejection = "no_match"
        if (record.num_offers == "1" or
                record.extent_competed_code.upper() in _NOT_COMPETED_CODES or
                record.other_than_full_open.strip()):
            rejection = "sole_source_unresolved"

        return self._make_result(primary_name, _normalize(primary_name),
                                  None, None, "none", "unresolved", rejection)

    # ── Tier 0: non-public detection ──────────────────────────────────────────

    def _is_non_public(self, record: ContractRecord) -> bool:
        country = record.country_of_incorporation.upper()
        if country and country != "USA":
            return True

        if any([
            record.is_educational_institution,
            record.is_federal_agency,
            record.is_airport_authority,
            record.is_council_of_governments,
            record.is_community_dev_corp,
            record.is_federally_funded_rd,
        ]):
            return True

        for pat in _NON_PUBLIC_RE:
            if pat.search(record.contractor_name) or pat.search(record.legal_business_name):
                return True

        return False

    # ── Tier 1: CAGE → SAM.gov Entity API → EDGAR ───────────────────────────────

    def _resolve_via_cage(self, record: ContractRecord) -> dict:
        """Attempt resolution via CAGE code → SAM.gov Entity API → canonical name → EDGAR.

        CAGE→GLEIF (Path A) was removed: GLEIF's registered_as field is a company
        registration number, not a CAGE code — audit showed 0.25% hit rate across
        399 lookups. Calling it for every entity wasted 1-2s per entity with no gain.
        """
        if not record.cage_code:
            return {}

        # CAGE → SAM.gov Entity API → canonical name → EDGAR
        entity = self.sam_entity_client.lookup_cage(record.cage_code)
        if entity and entity.get("legal_name"):
            canonical = entity["legal_name"]
            r = self._exact_match(canonical)
            if r:
                r["evidence_type"] = "cage_sam_" + r["evidence_type"]
                return r
            r = self._fuzzy_match(canonical)
            if r:
                r["evidence_type"] = "cage_sam_" + r["evidence_type"]
                return r

        return {}

    def _gleif_name_to_ticker(self, search_name: str, original_name: str,
                               norm: str, evidence_prefix: str) -> dict | None:
        """Search GLEIF by company name → LEI → OpenFIGI → ticker.

        Tries the full name and a 2-word prefix. Returns a result dict or None.
        Disabled for the session after repeated timeouts (circuit breaker).
        """
        global _gleif_disabled, _gleif_consecutive_timeouts
        if _gleif_disabled:
            return None

        candidates = [search_name]
        words = search_name.split()
        if len(words) > 2:
            candidates.append(" ".join(words[:2]))

        for candidate in candidates:
            if not candidate or len(candidate) < 3:
                continue
            try:
                params = {
                    "filter[entity.legalName.name]": candidate,
                    "page[size]": 5,
                }
                resp = requests.get(
                    "https://api.gleif.org/api/v1/lei-records",
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=4,
                )
                _gleif_consecutive_timeouts = 0  # reset on success
                if resp.status_code != 200:
                    continue
                for item in resp.json().get("data", []):
                    lei = item.get("attributes", {}).get("lei")
                    if not lei:
                        continue
                    lei_result = self.lei_resolver.resolve_lei(lei)
                    if lei_result.get("ticker"):
                        ticker = lei_result["ticker"]
                        mc = self._get_market_cap(ticker)
                        cik = lei_result.get("cik", "")
                        return self._make_result(
                            original_name, norm, ticker, cik,
                            "high", f"{evidence_prefix}_lei_openfigi", None, mc,
                        )
            except requests.exceptions.ConnectionError:
                log.debug("GLEIF API unreachable — disabling for session")
                _gleif_disabled = True
                return None
            except requests.exceptions.Timeout:
                _gleif_consecutive_timeouts += 1
                log.debug(f"GLEIF timeout #{_gleif_consecutive_timeouts} for '{candidate}'")
                if _gleif_consecutive_timeouts >= _GLEIF_TIMEOUT_THRESHOLD:
                    _gleif_disabled = True
                    log.warning("GLEIF API: too many timeouts — disabling for this session")
                    return None
                continue
            except Exception as e:
                log.debug(f"GLEIF name search error for '{candidate}': {e}")
                continue
        return None

    # ── Tier 2: multi-name EDGAR exact match ──────────────────────────────────

    def _exact_match(self, name: str) -> dict | None:
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)

        candidate = None
        for key in [name.strip().upper(), norm, stripped]:
            if key in self.edgar_map:
                candidate = self.edgar_map[key]
                break
        if not candidate and stripped in self._stripped_map:
            _, candidate = self._stripped_map[stripped]

        if not candidate:
            return None

        cik = candidate.get("cik", "")
        ticker = candidate["ticker"]
        if cik:
            valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
            if valid:
                mc = self._get_market_cap(ticker)
                return self._make_result(name, norm, ticker, cik, confidence, evidence, None, mc)
        else:
            mc = self._get_market_cap(ticker)
            return self._make_result(name, norm, ticker, "", "medium",
                                      "exact_edgar_map_unverified", None, mc)

    # ── Tier 3: multi-name fuzzy match ────────────────────────────────────────

    def _fuzzy_match(self, name: str) -> dict | None:
        if not self._edgar_names:
            return None
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)
        min_score = 70 if len(stripped.split()) <= 3 else 75

        results = process.extract(norm, self._edgar_names,
                                   scorer=fuzz.token_sort_ratio, limit=5)
        for match_name, score, _ in results:
            if score < min_score:
                break
            entry = self.edgar_map[match_name]
            cik = entry.get("cik", "")
            ticker = entry["ticker"]

            if score >= 95:
                mc = self._get_market_cap(ticker)
                return self._make_result(name, norm, ticker, cik,
                                          "medium_high", "fuzzy_very_high", None, mc)

            if cik:
                valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
                if valid:
                    mc = self._get_market_cap(ticker)
                    return self._make_result(name, norm, ticker, cik,
                                              confidence, f"fuzzy_{evidence}", None, mc)
            elif score >= 80:
                mc = self._get_market_cap(ticker)
                return self._make_result(name, norm, ticker, "", "low_medium",
                                          f"fuzzy_score_{int(score)}", None, mc)
        return None

    # ── Tier 4: substring match ───────────────────────────────────────────────

    def _substring_match(self, name: str) -> dict | None:
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)

        best_match = None
        best_len = 0
        for edgar_stripped, edgar_orig, entry in self._substr_candidates:
            match_len = 0
            if edgar_stripped in stripped:
                match_len = len(edgar_stripped)
            elif stripped in edgar_stripped:
                match_len = len(stripped)
            if match_len > best_len:
                best_match = (edgar_stripped, edgar_orig, entry)
                best_len = match_len

        if not best_match or best_len < 4:  # was 7; catches AECOM (5), SAIC (4), CACI (4)
            return None

        edgar_stripped, _, entry = best_match
        longer = max(len(stripped), len(edgar_stripped))
        if best_len / longer < 0.5:
            return None

        # Short matches must align on word boundaries to avoid false positives
        if best_len < 7:
            pattern = r"(?<![A-Z0-9])" + re.escape(edgar_stripped) + r"(?![A-Z0-9])"
            if not re.search(pattern, stripped):
                return None

        ticker = entry["ticker"]
        cik = entry.get("cik", "")
        mc = self._get_market_cap(ticker)
        return self._make_result(name, norm, ticker, cik, "medium",
                                  "substring_match", None, mc)

    # ── Market cap ────────────────────────────────────────────────────────────

    def _get_market_cap(self, ticker: str) -> float:
        if ticker in self.mcap_cache:
            return float(self.mcap_cache[ticker])
        try:
            mcap = float(yf.Ticker(ticker).fast_info.market_cap or 0)
            self.mcap_cache[ticker] = mcap
            self._mcap_unsaved += 1
            if self._mcap_unsaved >= _MCAP_CACHE_WRITE_BATCH:
                self._flush_mcap_cache()
            return mcap
        except Exception as e:
            log.debug(f"Market cap fetch failed for {ticker}: {e}")
            return 0.0

    # ── Result builder ────────────────────────────────────────────────────────

    @staticmethod
    def _make_result(original, normalized, ticker, cik, confidence,
                     evidence_type, rejection_reason=None, market_cap=0.0,
                     audit_trail=None) -> dict:
        return {
            "original_name":      original,
            "normalized_name":    normalized,
            "resolved_ticker":    ticker,
            "resolved_cik":       cik or "",
            "evidence_type":      evidence_type,
            "confidence":         confidence,
            "rejection_reason":   rejection_reason,
            "market_cap_current": market_cap or 0.0,
            "audit_trail":        audit_trail or [],
            "last_verified":      datetime.utcnow().isoformat(),
        }


# ── Module-level singleton (backward compat with main.py) ────────────────────
_resolver_instance: "TickerResolverV4 | None" = None


def resolve_ticker(awardee_name: str, edgar_results=None,
                   resolver: "TickerResolverV4 | None" = None,
                   cage_code: str = "") -> "tuple[str | None, str]":
    """Resolve awardee name → (ticker_or_None, confidence_str).

    Drop-in replacement for the V3 module-level function used by main.py.
    Builds a minimal ContractRecord and delegates to TickerResolverV4.
    """
    global _resolver_instance
    if resolver is None:
        if _resolver_instance is None:
            _resolver_instance = TickerResolverV4()
        resolver = _resolver_instance

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
