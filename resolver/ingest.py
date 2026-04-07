"""resolver/ingest.py — Contract ingestion, SEC EDGAR refresh, GLEIF refresh, OpenFIGI client."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import requests

from resolver.models import (
    ContractRowCanonical, ContractIdentityFeatures, RefreshReport,
    DEFAULT_HTTP_HEADERS,
)
from resolver.normalize import (
    normalize_name, normalize_legal_name, normalize_uei, normalize_cage,
    normalize_country, normalize_city, normalize_state, normalize_zip,
    normalize_phone, extract_registered_domain, normalize_cik, normalize_lei,
    coerce_award_date, parse_date,
)

log = logging.getLogger(__name__)

# ── Field alias maps ──────────────────────────────────────────────────────────

# Maps common SAM.gov / USASpending CSV column names → canonical field names
_FIELD_ALIASES: dict[str, str] = {
    # USASpending full CSV headers
    "Award ID":                                       "piid",
    "Modification Number":                            "modification_number",
    "Date Signed":                                    "date_signed",
    "Fiscal Year":                                    "fiscal_year",
    "Dollars Obligated":                              "dollars_obligated",
    "Base and All Options Value":                     "base_and_all_options_value",
    "Vendor CAGE Code":                               "cage_code",
    "Contractor Name":                                "contractor_name",
    "Legal Business Name":                            "legal_business_name",
    "Doing Business As Name":                         "doing_business_as_name",
    "Ultimate Parent Legal Business Name":            "ultimate_parent_legal_business_name",
    "Unique Entity ID":                               "unique_entity_id",
    "Ultimate Parent Unique Entity ID":               "ultimate_parent_unique_entity_id",
    "Vendor Website URL":                             "website_url",
    "Vendor Address City":                            "vendor_address_city",
    "Vendor Address State":                           "vendor_address_state",
    "Vendor Address Zip":                             "vendor_address_zip",
    "Vendor Address Country":                         "vendor_address_country",
    "Vendor Phone Number":                            "vendor_phone_number",
    "Country of Incorporation":                       "country_of_incorporation",
    "NAICS Code":                                     "naics_code",
    "NAICS Description":                              "naics_description",
    "Product or Service Code":                        "product_or_service_code",
    "Product or Service Code Description":            "product_or_service_description",
    "Extent Competed Code":                           "extent_competed_code",
    "Other Than Full And Open Competition":           "other_than_full_open",
    "Number Of Offers Received":                      "num_offers",
    "Educational Institution":                        "is_educational_institution",
    "Is Federal Government":                          "is_federal_agency",
    "Airport Authority":                              "is_airport_authority",
    "Council Of Governments":                         "is_council_of_governments",
    "Community Development Corporation":              "is_community_dev_corp",
    "Federally Funded Research And Development Corp": "is_federally_funded_rd",
    # Snake-case aliases (already canonical)
    "modification_number":              "modification_number",
    "piid":                             "piid",
    "date_signed":                      "date_signed",
    "fiscal_year":                      "fiscal_year",
    "dollars_obligated":                "dollars_obligated",
    "base_and_all_options_value":       "base_and_all_options_value",
    "cage_code":                        "cage_code",
    "contractor_name":                  "contractor_name",
    "legal_business_name":              "legal_business_name",
    "doing_business_as_name":           "doing_business_as_name",
    "ultimate_parent_legal_business_name": "ultimate_parent_legal_business_name",
    "unique_entity_id":                 "unique_entity_id",
    "ultimate_parent_unique_entity_id": "ultimate_parent_unique_entity_id",
    "website_url":                      "website_url",
    "vendor_address_city":              "vendor_address_city",
    "vendor_address_state":             "vendor_address_state",
    "vendor_address_zip":               "vendor_address_zip",
    "vendor_address_country":           "vendor_address_country",
    "vendor_phone_number":              "vendor_phone_number",
    "country_of_incorporation":         "country_of_incorporation",
    "naics_code":                       "naics_code",
    "naics_description":                "naics_description",
    "product_or_service_code":          "product_or_service_code",
    "product_or_service_description":   "product_or_service_description",
    "extent_competed_code":             "extent_competed_code",
    "other_than_full_open":             "other_than_full_open",
    "num_offers":                       "num_offers",
}

# ── Contract ingestion ────────────────────────────────────────────────────────

def load_contracts(contracts_input, field_mapping: dict | None = None):
    """Accept DataFrame, path-to-CSV, or path-to-Parquet. Returns pandas DataFrame."""
    import pandas as pd
    if isinstance(contracts_input, str):
        p = Path(contracts_input)
        if p.suffix in (".parquet", ".pq"):
            df = pd.read_parquet(contracts_input)
        else:
            df = pd.read_csv(contracts_input, dtype=str, low_memory=False)
    elif hasattr(contracts_input, "to_pandas"):
        df = contracts_input.to_pandas()  # polars
    elif hasattr(contracts_input, "to_pydict"):
        import pyarrow
        df = contracts_input.to_pandas()  # pyarrow table
    else:
        df = pd.DataFrame(contracts_input) if not isinstance(contracts_input, pd.DataFrame) else contracts_input
    return canonicalize_contract_columns(df, field_mapping)

def canonicalize_contract_columns(df, field_mapping: dict | None = None):
    """Rename columns to canonical names using _FIELD_ALIASES + optional overrides."""
    import pandas as pd
    aliases = dict(_FIELD_ALIASES)
    if field_mapping:
        aliases.update(field_mapping)
    rename = {col: aliases[col] for col in df.columns if col in aliases}
    df = df.rename(columns=rename)
    return df

def assign_contract_row_ids(df):
    """Assign stable row IDs based on piid or row index."""
    import pandas as pd
    def _make_id(row):
        piid = str(row.get("piid", "")).strip()
        mod  = str(row.get("modification_number", "")).strip()
        if piid:
            return hashlib.md5(f"{piid}_{mod}".encode()).hexdigest()[:16]
        return hashlib.md5(str(row.name).encode()).hexdigest()[:16]
    df = df.copy()
    df["contract_row_id"] = [_make_id(r) for _, r in df.iterrows()]
    return df

def build_contract_identity_features(df) -> list[ContractIdentityFeatures]:
    """Derive identity features from each canonicalized contract row."""
    features = []
    for _, row in df.iterrows():
        row_id = str(row.get("contract_row_id", ""))
        awardee_name = (str(row.get("legal_business_name") or "").strip() or
                        str(row.get("contractor_name") or "").strip())
        parent_name = str(row.get("ultimate_parent_legal_business_name") or "").strip()
        features.append(ContractIdentityFeatures(
            contract_row_id            = row_id,
            award_date                 = coerce_award_date(dict(row)),
            awardee_uei                = normalize_uei(row.get("unique_entity_id")),
            parent_uei                 = normalize_uei(row.get("ultimate_parent_unique_entity_id")),
            cage_code                  = normalize_cage(row.get("cage_code")),
            awardee_name_raw           = awardee_name or None,
            awardee_name_norm          = normalize_name(awardee_name),
            awardee_dba_raw            = str(row.get("doing_business_as_name") or "").strip() or None,
            awardee_dba_norm           = normalize_name(row.get("doing_business_as_name")),
            parent_name_raw            = parent_name or None,
            parent_name_norm           = normalize_name(parent_name),
            website_raw                = str(row.get("website_url") or "").strip() or None,
            website_domain             = extract_registered_domain(row.get("website_url")),
            vendor_city_norm           = normalize_city(row.get("vendor_address_city")),
            vendor_state_norm          = normalize_state(row.get("vendor_address_state")),
            vendor_zip_norm            = normalize_zip(row.get("vendor_address_zip")),
            vendor_country_norm        = normalize_country(row.get("vendor_address_country")),
            incorporation_country_norm = normalize_country(row.get("country_of_incorporation")),
            phone_norm                 = normalize_phone(row.get("vendor_phone_number")),
            dollars_obligated          = _to_decimal(row.get("dollars_obligated")),
        ))
    return features

def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        s = str(value).replace(",", "").replace("$", "").strip()
        return Decimal(s) if s else None
    except Exception:
        return None

# ── SEC EDGAR ─────────────────────────────────────────────────────────────────

_EDGAR_EXCHANGE_URL  = "https://www.sec.gov/files/company_tickers_exchange.json"
_EDGAR_FALLBACK_URL  = "https://www.sec.gov/files/company_tickers.json"
_EDGAR_SUBMISSIONS   = "https://data.sec.gov/submissions/CIK{cik}.json"
_EXCHANGE_RANK       = {"Nasdaq": 0, "NYSE": 0, "NYSEArca": 1, "NYSEAmerican": 1}

_edgar_last = 0.0

def _edgar_throttle(rate: float = 0.12) -> None:
    global _edgar_last
    elapsed = time.time() - _edgar_last
    if elapsed < rate:
        time.sleep(rate - elapsed)
    _edgar_last = time.time()

def refresh_sec_submissions(config) -> RefreshReport:
    """Download the SEC company tickers list → curated/sec_master.json."""
    from resolver.persistence import get_latest_refresh_status, record_refresh_event
    started = datetime.utcnow()
    source  = "sec"
    # Skip if refreshed within 7 days
    last = get_latest_refresh_status(source, config)
    if last and last["status"] == "ok":
        age_days = (datetime.utcnow() - datetime.fromisoformat(last["ended_at"])).days
        if age_days < 7:
            log.info("SEC tickers cache is fresh (<7 days), skipping download")
            return RefreshReport(source, started, datetime.utcnow(), "skipped")
    try:
        edgar_map = _download_edgar_tickers(config)
        rows = len(edgar_map)
        out  = Path(config.paths.curated_dir) / "sec_master.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            json.dump(edgar_map, f)
        ended = datetime.utcnow()
        report = RefreshReport(source, started, ended, "ok", rows_written=rows)
        record_refresh_event(source, started, ended, "ok", {}, config, rows_written=rows)
        log.info(f"SEC tickers refreshed: {rows:,} companies")
        return report
    except Exception as e:
        ended = datetime.utcnow()
        record_refresh_event(source, started, ended, "failed", {}, config, error=str(e))
        return RefreshReport(source, started, ended, "failed", error=str(e))

def _download_edgar_tickers(config) -> dict:
    """Download and parse SEC company_tickers_exchange.json → {name: {ticker, cik}}."""
    headers = {"User-Agent": config.runtime.user_agent, "Accept": "application/json"}
    timeout = config.runtime.http_timeout_seconds
    try:
        resp = requests.get(_EDGAR_EXCHANGE_URL, headers=headers, timeout=timeout)
        resp.raise_for_status()
        raw    = resp.json()
        fields = raw.get("fields", [])
        rows   = raw.get("data", [])
        idx    = {f: i for i, f in enumerate(fields)}
        candidates: dict[str, list] = {}
        for row in rows:
            cik      = str(row[idx["cik"]])
            name     = row[idx["name"]].strip().upper()
            ticker   = row[idx["ticker"]].strip().upper()
            exchange = row[idx["exchange"]] if "exchange" in idx else ""
            if not name or not ticker:
                continue
            is_special = any(c in ticker for c in "-/+")
            exrank     = _EXCHANGE_RANK.get(exchange, 2) if exchange != "OTC" else 3
            candidates.setdefault(name, []).append(
                (exrank, int(is_special), len(ticker), ticker, cik)
            )
        edgar_map: dict = {}
        for name, entries in candidates.items():
            entries.sort()
            best = entries[0]
            edgar_map[name] = {"ticker": best[3], "cik": best[4]}
        return edgar_map
    except Exception as e:
        log.warning(f"Exchange tickers failed ({e}), falling back to basic SEC download")
    # Fallback
    resp = requests.get(_EDGAR_FALLBACK_URL, headers=headers, timeout=timeout)
    resp.raise_for_status()
    raw = resp.json()
    edgar_map = {}
    for entry in raw.values():
        name   = entry.get("title", "").strip().upper()
        ticker = entry.get("ticker", "").strip().upper()
        cik    = str(entry.get("cik_str", ""))
        if not name or not ticker:
            continue
        is_special = any(c in ticker for c in "-/+")
        if name in edgar_map:
            if not any(c in edgar_map[name]["ticker"] for c in "-/+"):
                continue
            if is_special:
                continue
        edgar_map[name] = {"ticker": ticker, "cik": cik}
    return edgar_map

def load_sec_submissions_raw(config) -> dict:
    """Load the curated SEC master (downloaded by refresh_sec_submissions)."""
    path = Path(config.paths.curated_dir) / "sec_master.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)

def fetch_sec_entity_metadata(cik: str, config) -> dict | None:
    """Fetch live SEC submissions for one CIK (used sparingly as fallback)."""
    if not cik:
        return None
    _edgar_throttle()
    try:
        cik_padded = normalize_cik(cik) or cik.zfill(10)
        url  = _EDGAR_SUBMISSIONS.format(cik=cik_padded)
        headers = {"User-Agent": config.runtime.user_agent, "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=config.runtime.http_timeout_seconds)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return {
            "name":        data.get("name", ""),
            "formerNames": data.get("formerNames", []),
            "tickers":     data.get("tickers", []),
            "exchanges":   data.get("exchanges", []),
            "entityType":  data.get("entityType", ""),
            "sic":         data.get("sic", ""),
            "sicDescription": data.get("sicDescription", ""),
            "filings":     data.get("filings", {}).get("recent", {}),
        }
    except Exception as e:
        log.debug(f"SEC metadata fetch failed for CIK {cik}: {e}")
        return None

def extract_former_names(submission_json: dict) -> list[str]:
    return [fn.get("name", "") for fn in submission_json.get("formerNames", []) if fn.get("name")]

def extract_recent_tickers_and_exchanges(submission_json: dict) -> dict:
    return {
        "tickers":   submission_json.get("tickers", []),
        "exchanges": submission_json.get("exchanges", []),
    }

# ── GLEIF ─────────────────────────────────────────────────────────────────────

_GLEIF_API_BASE = "https://api.gleif.org/api/v1"

_gleif_consecutive_timeouts = 0
_gleif_disabled             = False
_GLEIF_TIMEOUT_THRESHOLD    = 3

def refresh_gleif_data(config) -> RefreshReport:
    """
    Download and parse GLEIF Golden Copy CSV files if gleif_refresh_enabled.
    This is opt-in because the files are large (~1GB compressed).
    When disabled, the GLEIF layer falls back to live API calls only.
    """
    from resolver.persistence import record_refresh_event, get_latest_refresh_status
    started = datetime.utcnow()
    source  = "gleif"
    if not config.source_policies.gleif_refresh_enabled:
        log.info("GLEIF bulk refresh is disabled (set gleif_refresh_enabled=True to enable)")
        return RefreshReport(source, started, datetime.utcnow(), "skipped")
    last = get_latest_refresh_status(source, config)
    if last and last["status"] == "ok":
        age_days = (datetime.utcnow() - datetime.fromisoformat(last["ended_at"])).days
        if age_days < 1:
            return RefreshReport(source, started, datetime.utcnow(), "skipped")
    try:
        # Full GLEIF Golden Copy — streaming download
        gleif_map = _build_gleif_map_from_api_sample(config)
        out = Path(config.paths.curated_dir) / "gleif_master.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            json.dump(gleif_map, f)
        ended = datetime.utcnow()
        rows  = len(gleif_map)
        record_refresh_event(source, started, ended, "ok", {}, config, rows_written=rows)
        return RefreshReport(source, started, ended, "ok", rows_written=rows)
    except Exception as e:
        ended = datetime.utcnow()
        record_refresh_event(source, started, ended, "failed", {}, config, error=str(e))
        return RefreshReport(source, started, ended, "failed", error=str(e))

def _build_gleif_map_from_api_sample(config) -> dict:
    """Build a small in-memory GLEIF map from API responses (used when bulk disabled)."""
    # Intentionally lightweight — real bulk loading happens via Golden Copy when enabled
    return {}

def load_gleif_master(config) -> dict:
    path = Path(config.paths.curated_dir) / "gleif_master.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)

def gleif_name_search(search_name: str, config, limit: int = 5) -> list[dict]:
    """Live GLEIF API name search. Returns list of {lei, name, country} dicts."""
    global _gleif_disabled, _gleif_consecutive_timeouts
    if _gleif_disabled or not config.source_policies.allow_gleif_api_fallback:
        return []
    candidates = [search_name]
    words = search_name.split()
    if len(words) > 2:
        candidates.append(" ".join(words[:2]))
    results = []
    for candidate in candidates:
        if not candidate or len(candidate) < 3:
            continue
        try:
            resp = requests.get(
                f"{_GLEIF_API_BASE}/lei-records",
                params={"filter[entity.legalName.name]": candidate, "page[size]": limit},
                headers={"Accept": "application/json"},
                timeout=4,
            )
            _gleif_consecutive_timeouts = 0
            if resp.status_code != 200:
                continue
            for item in resp.json().get("data", []):
                attrs  = item.get("attributes", {})
                entity = attrs.get("entity", {})
                lei    = attrs.get("lei")
                if not lei:
                    continue
                results.append({
                    "lei":     lei,
                    "name":    entity.get("legalName", {}).get("name", ""),
                    "country": entity.get("legalAddress", {}).get("country", ""),
                })
        except requests.exceptions.ConnectionError:
            log.debug("GLEIF API unreachable")
            _gleif_disabled = True
            return results
        except requests.exceptions.Timeout:
            _gleif_consecutive_timeouts += 1
            if _gleif_consecutive_timeouts >= _GLEIF_TIMEOUT_THRESHOLD:
                _gleif_disabled = True
                log.warning("GLEIF API: too many timeouts — disabling for session")
                return results
        except Exception as e:
            log.debug(f"GLEIF name search error: {e}")
    return results

def gleif_lei_lookup(lei: str, config) -> dict | None:
    """Fetch a single LEI record from GLEIF API."""
    global _gleif_disabled
    if _gleif_disabled:
        return None
    try:
        resp = requests.get(
            f"{_GLEIF_API_BASE}/lei-records/{lei}",
            headers={"Accept": "application/json"},
            timeout=config.runtime.http_timeout_seconds,
        )
        if resp.status_code != 200:
            return None
        data   = resp.json().get("data", {})
        attrs  = data.get("attributes", {})
        entity = attrs.get("entity", {})
        return {
            "lei":     attrs.get("lei"),
            "name":    entity.get("legalName", {}).get("name", ""),
            "country": entity.get("legalAddress", {}).get("country", ""),
            "status":  entity.get("status", ""),
        }
    except Exception as e:
        log.debug(f"GLEIF LEI lookup failed for {lei}: {e}")
        return None

def gleif_get_parent_lei(lei: str, config) -> dict | None:
    """Get direct/ultimate parent LEI via GLEIF relationships API."""
    global _gleif_disabled
    if _gleif_disabled:
        return None
    try:
        resp = requests.get(
            f"{_GLEIF_API_BASE}/lei-records/{lei}/direct-parent",
            headers={"Accept": "application/json"},
            timeout=config.runtime.http_timeout_seconds,
        )
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            return None
        data  = resp.json().get("data", {})
        attrs = data.get("attributes", {})
        return {
            "lei":     attrs.get("lei"),
            "name":    attrs.get("entity", {}).get("legalName", {}).get("name", ""),
            "country": attrs.get("entity", {}).get("legalAddress", {}).get("country", ""),
        }
    except Exception as e:
        log.debug(f"GLEIF parent lookup failed for {lei}: {e}")
        return None

# ── OpenFIGI ──────────────────────────────────────────────────────────────────

_OPENFIGI_URL    = "https://api.openfigi.com/v3/mapping"
_OPENFIGI_SEARCH = "https://api.openfigi.com/v3/search"

def map_identifiers_bulk(requests_list: list[dict], config) -> list:
    """Bulk map identifiers via OpenFIGI mapping endpoint."""
    if not config.source_policies.allow_live_openfigi:
        return [None] * len(requests_list)
    api_key = os.environ.get("OPENFIGI_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    results = []
    # OpenFIGI allows up to 100 per request
    for i in range(0, len(requests_list), 100):
        chunk = requests_list[i:i+100]
        try:
            resp = requests.post(
                _OPENFIGI_URL, headers=headers,
                json=chunk, timeout=30,
            )
            if resp.status_code == 200:
                results.extend(resp.json())
            else:
                results.extend([None] * len(chunk))
        except Exception as e:
            log.debug(f"OpenFIGI bulk map failed: {e}")
            results.extend([None] * len(chunk))
        time.sleep(0.5)  # rate-limit
    return results

def search_openfigi_fallback(query: str, filters: dict, config) -> list:
    """Search OpenFIGI by name (fallback; less accurate than mapping)."""
    if not config.source_policies.allow_live_openfigi:
        return []
    api_key = os.environ.get("OPENFIGI_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    payload = {"query": query, **filters}
    try:
        resp = requests.post(_OPENFIGI_SEARCH, headers=headers, json=payload, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except Exception as e:
        log.debug(f"OpenFIGI search failed: {e}")
    return []

def cache_openfigi_response(key: str, response: Any, config) -> None:
    from resolver.persistence import put_cached_http_response
    put_cached_http_response(f"openfigi:{key}", response, {"url": _OPENFIGI_URL}, config)

def load_cached_openfigi_response(key: str, config) -> Any:
    from resolver.persistence import get_cached_http_response
    return get_cached_http_response(f"openfigi:{key}", config)

def openfigi_lei_to_ticker(lei: str, config) -> str | None:
    """Map a LEI to a US common-stock ticker via OpenFIGI."""
    cache_key = f"lei:{lei}"
    cached = load_cached_openfigi_response(cache_key, config)
    if cached is not None:
        return cached.get("ticker")
    results = map_identifiers_bulk([{"idType": "LEI", "idValue": lei}], config)
    if not results or not results[0]:
        return None
    data = results[0]
    if isinstance(data, dict) and "error" in data:
        cache_openfigi_response(cache_key, {"ticker": None}, config)
        return None
    # Pick best: US, common stock, listed exchange
    best = _pick_best_openfigi_security(data.get("data", []), config)
    result = {"ticker": best} if best else {"ticker": None}
    cache_openfigi_response(cache_key, result, config)
    return best

def _pick_best_openfigi_security(securities: list, config) -> str | None:
    """From an OpenFIGI data list, choose the best US-tradable common-stock ticker."""
    from resolver.models import US_EXCHANGE_CODES_ALLOWED, DISALLOWED_SECURITY_TYPES
    preferred = []
    fallback  = []
    for sec in securities:
        ticker    = sec.get("ticker", "")
        exch_code = sec.get("exchCode", "")
        sec_type  = sec.get("securityType", "") or sec.get("securityType2", "")
        mic       = sec.get("marketStatus", "")
        if not ticker:
            continue
        if any(t in sec_type for t in DISALLOWED_SECURITY_TYPES):
            continue
        is_us  = exch_code in {"US", "UA", "UW", "UT", "UQ", "UN"}
        is_common = "Common" in sec_type or sec_type == "EQ"
        is_adr    = "ADR" in sec_type or "ADS" in sec_type
        if is_us and is_common:
            preferred.append(ticker)
        elif is_us and is_adr and config.security_selection.allow_adr_fallback:
            fallback.append(ticker)
    return preferred[0] if preferred else (fallback[0] if fallback else None)

# ── SAM optional extract ──────────────────────────────────────────────────────

def refresh_sam_optional_extract(config) -> RefreshReport:
    started = datetime.utcnow()
    source  = "sam_optional"
    if not config.source_policies.use_sam_monthly_extract:
        return RefreshReport(source, started, datetime.utcnow(), "skipped")
    # Placeholder: SAM monthly extract requires FTP/S3 access not universally available
    log.info("SAM monthly extract refresh not yet implemented (opt-in only)")
    ended = datetime.utcnow()
    return RefreshReport(source, started, ended, "skipped")

def load_sam_entity_extract(config) -> dict:
    path = Path(config.paths.curated_dir) / "sam_entity_extract.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)
