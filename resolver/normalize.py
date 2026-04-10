"""resolver/normalize.py — Names, domains, addresses, identifiers, dates."""
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Any

try:
    import tldextract as _tld
    _TLD_AVAILABLE = True
except ImportError:
    _TLD_AVAILABLE = False

from resolver.models import LEGAL_SUFFIXES, COMMON_STOPWORDS, NON_PUBLIC_PATTERNS

# ── Compiled patterns ─────────────────────────────────────────────────────────

_NON_PUBLIC_RE = [re.compile(p, re.IGNORECASE) for p in NON_PUBLIC_PATTERNS]
_NONPROFIT_RE  = re.compile(
    r"\b(NONPROFIT|NON-PROFIT|NOT[ -]FOR[ -]PROFIT|CHARITY|CHARITABLE"
    r"|CHURCH|TEMPLE|MOSQUE|SYNAGOGUE|DIOCESE|PARISH)\b",
    re.IGNORECASE,
)
_UNIVERSITY_RE = re.compile(
    r"\b(UNIVERSIT|COLLEGE|SCHOOL OF|INSTITUTE OF TECHNOLOGY"
    r"|COMMUNITY COLLEGE|POLYTECHNIC)\b",
    re.IGNORECASE,
)
_GOVT_RE = re.compile(
    r"\b(DEPARTMENT OF|BUREAU OF|OFFICE OF|NATIONAL LABORATOR)",
    re.IGNORECASE,
)

# ── Names ─────────────────────────────────────────────────────────────────────

def normalize_name(value: str | None) -> str | None:
    """Uppercase, remove punctuation except spaces, collapse whitespace."""
    if not value or not value.strip():
        return None
    # Unicode normalize
    s = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    s = s.strip().upper()
    s = s.replace("&", "AND")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r" +", " ", s).strip() or None

def normalize_legal_name(value: str | None) -> str | None:
    return normalize_name(value)

def normalize_alias_name(value: str | None) -> str | None:
    return normalize_name(value)

def strip_legal_suffixes(value: str | None) -> str | None:
    if not value:
        return None
    words = value.split()
    while words and words[-1] in LEGAL_SUFFIXES:
        words.pop()
    result = " ".join(words)
    return result if result else value

def name_tokens(value: str | None) -> list[str]:
    if not value:
        return []
    norm = normalize_name(value)
    if not norm:
        return []
    return [w for w in norm.split() if w not in COMMON_STOPWORDS]

def name_acronym(value: str | None) -> str | None:
    tokens = name_tokens(value)
    if len(tokens) < 2:
        return None
    return "".join(t[0] for t in tokens)

def build_name_variants(raw_name: str | None) -> dict:
    """Return a dict with normalized forms for matching."""
    norm    = normalize_name(raw_name)
    stripped = strip_legal_suffixes(norm) if norm else None
    tokens  = name_tokens(raw_name)
    acronym = name_acronym(raw_name)
    return {
        "raw":        raw_name,
        "norm":       norm,
        "stripped":   stripped,
        "tokens":     tokens,
        "acronym":    acronym,
        "is_acronym_only": bool(norm and len(norm) <= 5 and re.fullmatch(r"[A-Z0-9]+", norm)),
    }

def looks_like_government_entity(value: str | None) -> bool:
    if not value:
        return False
    return bool(_GOVT_RE.search(value))

def looks_like_nonprofit_or_university(value: str | None) -> bool:
    if not value:
        return False
    return bool(_NONPROFIT_RE.search(value) or _UNIVERSITY_RE.search(value))

def is_non_public_name(name: str | None) -> bool:
    if not name:
        return False
    for pat in _NON_PUBLIC_RE:
        if pat.search(name):
            return True
    return False

# ── Domains ───────────────────────────────────────────────────────────────────

def normalize_url(value: str | None) -> str | None:
    if not value or not value.strip():
        return None
    v = value.strip()
    if not re.match(r"^https?://", v, re.IGNORECASE):
        v = "http://" + v
    return v

def extract_registered_domain(value: str | None) -> str | None:
    if not value or not value.strip():
        return None
    url = normalize_url(value)
    if not url:
        return None
    if _TLD_AVAILABLE:
        ext = _tld.extract(url)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}".lower()
    # Fallback: simple regex
    m = re.search(r"(?:https?://)?(?:www\.)?([^/?\s]+)", url, re.IGNORECASE)
    if m:
        return m.group(1).lower().split(":")[0]
    return None

def domain_tokens(domain: str | None) -> list[str]:
    if not domain:
        return []
    parts = domain.split(".")
    return [p for p in parts if p and p not in ("com", "net", "org", "gov", "edu", "io", "www")]

# ── Addresses ─────────────────────────────────────────────────────────────────

_US_STATE_ABBREVS = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
    "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC",
}

def normalize_city(value: str | None) -> str | None:
    if not value:
        return None
    return re.sub(r"\s+", " ", value.strip().upper()) or None

def normalize_state(value: str | None) -> str | None:
    if not value:
        return None
    s = value.strip().upper()
    if len(s) == 2:
        return s
    return _US_STATE_ABBREVS.get(s, s)

def normalize_zip(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"[^0-9]", "", value)
    return digits[:5] if len(digits) >= 5 else digits or None

def normalize_country(value: str | None) -> str | None:
    if not value:
        return None
    s = value.strip().upper()
    # map common aliases
    aliases = {
        "UNITED STATES": "USA", "UNITED STATES OF AMERICA": "USA", "US": "USA",
        "U.S.": "USA", "U.S.A.": "USA",
        "UNITED KINGDOM": "GBR", "UK": "GBR", "GREAT BRITAIN": "GBR",
        "CANADA": "CAN", "GERMANY": "DEU", "FRANCE": "FRA",
        "AUSTRALIA": "AUS", "ISRAEL": "ISR",
    }
    return aliases.get(s, s)

def build_address_signature(
    city: str | None, state: str | None, zip_code: str | None, country: str | None
) -> str | None:
    parts = [
        normalize_city(city),
        normalize_state(state),
        normalize_zip(zip_code),
        normalize_country(country),
    ]
    parts = [p for p in parts if p]
    return "|".join(parts) if parts else None

def address_similarity(left: dict, right: dict) -> float:
    """0–1 similarity between two address dicts (city, state, zip, country)."""
    score = 0.0
    total = 0.0
    for key, weight in [("country", 0.3), ("state", 0.3), ("city", 0.25), ("zip", 0.15)]:
        lv = left.get(key)
        rv = right.get(key)
        if lv and rv:
            total += weight
            if normalize_name(lv) == normalize_name(rv):
                score += weight
    return score / total if total > 0 else 0.0

# ── Identifiers ───────────────────────────────────────────────────────────────

def normalize_uei(value: str | None) -> str | None:
    if not value:
        return None
    s = re.sub(r"[^A-Z0-9]", "", value.strip().upper())
    return s if s else None

def normalize_cage(value: str | None) -> str | None:
    if not value:
        return None
    s = re.sub(r"[^A-Z0-9]", "", value.strip().upper())
    return s if s else None

def normalize_phone(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"[^0-9]", "", value)
    return digits[-10:] if len(digits) >= 10 else None

def normalize_cik(value: str | None) -> str | None:
    if not value:
        return None
    s = re.sub(r"[^0-9]", "", str(value))
    return s.zfill(10) if s else None

def normalize_lei(value: str | None) -> str | None:
    if not value:
        return None
    s = re.sub(r"[^A-Z0-9]", "", value.strip().upper())
    return s if len(s) == 20 else None

# ── Dates ─────────────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%d-%b-%Y",
    "%B %d, %Y", "%m-%d-%Y", "%Y/%m/%d",
]

def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def coerce_award_date(row: dict) -> date | None:
    """Try date_signed, then Period of Performance Start Date, then fiscal_year."""
    for field in ("date_signed", "Date Signed", "award_date", "period_of_performance_start_date"):
        v = row.get(field)
        d = parse_date(v)
        if d:
            return d
    fy = row.get("fiscal_year") or row.get("Fiscal Year")
    if fy:
        try:
            return date(int(fy), 10, 1)  # Oct 1 = start of US fiscal year
        except (ValueError, TypeError):
            pass
    return None

def historical_symbol_coverage_bucket(value: date | None) -> str | None:
    """Categorise a date into coverage-confidence bucket."""
    if value is None:
        return None
    cutoff_high = date(2021, 6, 15)
    cutoff_mid  = date(2019, 6, 15)
    if value >= cutoff_high:
        return "post_2021_06_15"
    if value >= cutoff_mid:
        return "2019_06_15_to_2021_06_14"
    return "pre_2019_06_15"
