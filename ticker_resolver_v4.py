"""TickerResolverV4 — multi-tier resolver using SAM.gov ContractRecord fields.

Resolution pipeline (stops at first hit):
  Tier 0: Hard rejects (non-public flags, foreign country, name regex)
  Tier 1: CAGE → GLEIF → LEI → OpenFIGI
  Tier 2: Multi-name EDGAR exact match (4 names: legal, contractor, dba, parent)
  Tier 3: Multi-name EDGAR fuzzy match (4 names, threshold 85)
  Tier 4: Substring match (catch subsidiaries)
  Tier 5: Sole-source tag (num_offers == "1" or not-competed → tag for scorer)

Improvements over V3:
  - Accepts ContractRecord (rich data) instead of a bare name string
  - Four name attempts per tier (legal > contractor > dba > parent)
  - Business-type flags enable zero-API non-public detection
  - Cache key uses CAGE/UEI for stable identity across name variations
  - Separate cache file (.ticker_cache_v4.json) — no V3 collision
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import yfinance as yf
from rapidfuzz import fuzz, process

from cage_resolver import CageResolver
from lei_resolver import LeiResolver
from sam_gov_reader import ContractRecord

# Reuse shared EDGAR utilities from V3 (avoids duplication)
from ticker_resolver_v3 import (
    _load_edgar_map_default,
    _normalize,
    _strip_suffixes,
    _validate_candidate,
    _NON_PUBLIC_RE,
)

log = logging.getLogger(__name__)

_MCAP_CACHE_WRITE_BATCH = 50

# Extent Competed codes indicating no competition (sole source)
_NOT_COMPETED_CODES = {"B", "C", "G", "CDO", "URG", "SP2"}


class TickerResolverV4:
    """5-tier resolver operating on ContractRecord from sam_gov_reader."""

    def __init__(self, edgar_map: dict | None = None,
                 cache_path: str = ".ticker_cache_v4.json",
                 mcap_cache_path: str = ".mcap_cache.json"):
        if edgar_map is None:
            edgar_map = _load_edgar_map_default()
        self.edgar_map = edgar_map
        self.cache_path = cache_path
        self.mcap_cache_path = mcap_cache_path
        self.cache: dict = {}
        self.mcap_cache: dict = {}
        self._mcap_unsaved = 0
        self.cage_resolver = CageResolver()
        self.lei_resolver = LeiResolver()

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
            if s and len(s.split()) >= 2:
                self._substr_candidates.append((s, ename, entry))

    # ── Cache I/O ────────────────────────────────────────────────────────────

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

        # Tier 1: CAGE → Company Name → GLEIF → LEI → OpenFIGI
        if record.cage_code:
            r = self._resolve_via_cage(record)
            if r.get("resolved_ticker"):
                return r

        # Names to try in Tiers 2–4 (skip empty strings)
        names = [n for n in [
            record.legal_business_name,
            record.contractor_name,
            record.dba_name,
            record.parent_name,
        ] if n.strip()]

        # Tier 2: multi-name exact match
        for name in names:
            r = self._exact_match(name)
            if r:
                return r

        # Tier 3: multi-name fuzzy match (lowered threshold)
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

        # Tier 5: sole-source tag
        rejection = "no_match"
        if (record.num_offers == "1" or
                record.extent_competed_code.upper() in _NOT_COMPETED_CODES or
                record.other_than_full_open.strip()):
            rejection = "sole_source_unresolved"

        return self._make_result(primary_name, _normalize(primary_name),
                                  None, None, "none", "unresolved", rejection)

    # ── Tier 0: non-public detection ─────────────────────────────────────────

    def _is_non_public(self, record: ContractRecord) -> bool:
        # Defensive foreign-country check (reader already filters, but be safe)
        country = record.country_of_incorporation.upper()
        if country and country != "USA":
            return True

        # Business-type flags
        if any([
            record.is_educational_institution,
            record.is_federal_agency,
            record.is_airport_authority,
            record.is_council_of_governments,
            record.is_community_dev_corp,
            record.is_federally_funded_rd,
        ]):
            return True

        # Name-regex check (universities, counties, state agencies, etc.)
        for pat in _NON_PUBLIC_RE:
            if pat.search(record.contractor_name) or pat.search(record.legal_business_name):
                return True

        return False

    # ── Tier 1: CAGE → Company Name → LEI → OpenFIGI ────────────────────────────

    def _resolve_via_cage(self, record: ContractRecord) -> dict:
        """Try to resolve via CAGE code using GLEIF name search → LEI → OpenFIGI.

        Note: GLEIF API requires internet connectivity. Falls back to EDGAR tiers
        if GLEIF is unavailable.
        """
        if not record.cage_code:
            return {}

        primary_name = record.contractor_name or record.legal_business_name

        # Build list of names to try (original + normalized variations)
        names_to_try = []
        for name in [record.contractor_name, record.legal_business_name, record.dba_name, record.parent_name]:
            if name and name.strip():
                names_to_try.append(name.strip())
                # Also try without common suffixes
                normalized = _strip_suffixes(_normalize(name))
                if normalized and normalized not in names_to_try:
                    names_to_try.append(normalized)

        # If no names, Tier 1 can't proceed (fall through to Tiers 2-4)
        if not names_to_try:
            return {}

        for name in names_to_try:
            try:
                import requests
                # Try searching GLEIF by company name (try exact and partial matches)
                for search_name in [name, name.split()[0:2]]:  # Try full name and first 2 words
                    if isinstance(search_name, list):
                        search_name = " ".join(search_name)
                    if not search_name or len(search_name) < 3:
                        continue

                    params = {
                        "filter[registered_as]": search_name,
                        "page[size]": 5
                    }
                    resp = requests.get(
                        "https://leilookup.gleif.org/api/v3/lei-records",
                        params=params,
                        headers={"Accept": "application/json"},
                        timeout=10
                    )

                    if resp.status_code == 200:
                        data = resp.json()
                        records = data.get("lei_records", [])

                        # Try each returned LEI until one resolves to a ticker
                        for record_item in records:
                            lei = record_item.get("lei")
                            if lei:
                                # Now resolve LEI to ticker
                                lei_result = self.lei_resolver.resolve_lei(lei)
                                if lei_result.get("ticker"):
                                    ticker = lei_result["ticker"]
                                    cik = lei_result.get("cik", "")
                                    mc = self._get_market_cap(ticker)
                                    norm = _normalize(primary_name)
                                    return self._make_result(primary_name, norm, ticker, cik, "high",
                                                            "cage_gleif_lei_openfigi", None, mc)
                    else:
                        log.debug(f"Tier 1 GLEIF API error for '{search_name}': HTTP {resp.status_code}")
            except requests.exceptions.ConnectionError as e:
                # GLEIF unreachable (no internet or DNS failure) — log once, fall through
                log.debug(f"Tier 1 GLEIF unreachable (network): {type(e).__name__}")
                return {}
            except requests.exceptions.Timeout:
                log.debug(f"Tier 1 GLEIF timeout for '{name}'")
                continue
            except Exception as e:
                # Unexpected error — log and continue to next name
                log.debug(f"Tier 1 error for CAGE {record.cage_code} / '{name}': {type(e).__name__}: {e}")
                continue

        # Tier 1 could not resolve (GLEIF unavailable, no matches, or API errors)
        # Fall through to Tiers 2-4 (EDGAR exact/fuzzy/substring)
        return {}

    # ── Tier 2: multi-name EDGAR exact match ─────────────────────────────────

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
            if mc > 0:
                return self._make_result(name, norm, ticker, "", "medium",
                                          "exact_edgar_map_unverified", None, mc)
        return None

    # ── Tier 3: multi-name fuzzy match ───────────────────────────────────────

    def _fuzzy_match(self, name: str) -> dict | None:
        if not self._edgar_names:
            return None
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)
        # Lowered threshold from 80/85 to 70/75 to catch more matches
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
                if mc > 0:
                    return self._make_result(name, norm, ticker, cik,
                                              "medium_high", "fuzzy_very_high", None, mc)

            if cik:
                valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
                if valid:
                    mc = self._get_market_cap(ticker)
                    return self._make_result(name, norm, ticker, cik,
                                              confidence, f"fuzzy_{evidence}", None, mc)
            elif score >= 80:
                # Accept without CIK validation if score is high enough
                mc = self._get_market_cap(ticker)
                if mc > 0:
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

        if not best_match or best_len < 7:
            return None

        edgar_stripped, _, entry = best_match
        longer = max(len(stripped), len(edgar_stripped))
        if best_len / longer < 0.5:
            return None

        ticker = entry["ticker"]
        cik = entry.get("cik", "")
        mc = self._get_market_cap(ticker)
        if mc > 0:
            return self._make_result(name, norm, ticker, cik, "medium",
                                      "substring_match", None, mc)
        return None

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
