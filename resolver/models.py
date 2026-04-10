"""resolver/models.py — All typed models, enums, constants, exceptions, and config."""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

# ── Exceptions ────────────────────────────────────────────────────────────────

class ResolverError(Exception):
    pass

class ConfigError(ResolverError):
    pass

class RefreshError(ResolverError):
    pass

class NormalizationError(ResolverError):
    pass

class EntityBuildError(ResolverError):
    pass

class CandidateGenerationError(ResolverError):
    pass

class HistoricalSymbolError(ResolverError):
    pass

class SecuritySelectionError(ResolverError):
    pass

class ReferenceDataMissingError(ResolverError):
    pass

# ── Enums ─────────────────────────────────────────────────────────────────────

class ResolverStatus(str, Enum):
    RESOLVED                       = "resolved"
    NULL_PRIVATE                   = "null_private"
    NULL_NO_PUBLIC_PARENT          = "null_no_public_parent"
    NULL_AMBIGUOUS_ENTITY          = "null_ambiguous_entity"
    NULL_NO_US_TRADABLE_SECURITY   = "null_no_us_tradable_security"
    NULL_HISTORICAL_UNAVAILABLE    = "null_historical_symbol_unavailable"
    NULL_LOW_CONFIDENCE            = "null_low_confidence"
    NULL_BAD_INPUT                 = "null_bad_input"
    NULL_ERROR                     = "null_error"

class EntityType(str, Enum):
    AWARDEE = "awardee"
    PARENT  = "parent"

class MatchSource(str, Enum):
    SEC_EXACT          = "sec_exact"
    SEC_FORMER_NAME    = "sec_former_name"
    GLEIF_DIRECT       = "gleif_direct"
    GLEIF_PARENT_CHAIN = "gleif_parent_chain"
    LEI_ISIN_FIGI      = "lei_isin_figi"
    FALLBACK           = "fallback"

class SecurityTypeClass(str, Enum):
    COMMON_STOCK = "Common Stock"
    ADR          = "ADR"
    PREFERRED    = "Preferred"
    WARRANT      = "Warrant"
    FUND         = "Fund"
    BOND         = "Bond"
    OTHER        = "Other"

class NullReason(str, Enum):
    NON_PUBLIC_ENTITY          = "non_public_entity"
    FOREIGN_ENTITY             = "foreign_entity"
    NO_MATCH                   = "no_match"
    SOLE_SOURCE_UNRESOLVED     = "sole_source_unresolved"
    AMBIGUOUS                  = "ambiguous"
    NO_US_SECURITY             = "no_us_tradable_security"
    HISTORICAL_UNAVAILABLE     = "historical_symbol_unavailable"
    HISTORICAL_CONFLICT        = "historical_symbol_conflict"
    HISTORICAL_LOW_CONFIDENCE  = "historical_symbol_low_confidence"
    LOW_CONFIDENCE             = "low_confidence"
    BAD_INPUT                  = "bad_input"
    ERROR                      = "error"
    OVERRIDE_FORCED_NULL       = "override_forced_null"

# ── Constants ─────────────────────────────────────────────────────────────────

LEGAL_SUFFIXES = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "LLC", "LLP",
    "LTD", "LIMITED", "CO", "COMPANY", "LP", "HOLDINGS",
    "GROUP", "TECHNOLOGIES", "SOLUTIONS", "SYSTEMS", "SERVICES",
    "ENTERPRISES", "GLOBAL", "USA", "US", "DBA",
    "DE", "MD", "NV", "NY", "VA", "CA", "TX", "FL", "PA", "OH",
    "WA", "GA", "MA", "IL", "NJ", "CT", "AZ", "MN",
}

COMMON_STOPWORDS = {
    "THE", "AND", "OF", "IN", "FOR", "A", "AN", "TO", "BY",
}

US_EXCHANGE_CODES_ALLOWED = {"Nasdaq", "NYSE", "NYSEArca", "NYSEAmerican", "BATS", "CBOE"}

DISALLOWED_SECURITY_TYPES = {
    "Preferred Stock", "Warrant", "Unit", "Right", "Note",
    "Fund", "ETF", "ETN", "Bond", "Debenture", "Derivative",
}

DEFAULT_SCORE_WEIGHTS = {
    "name_exact":           50.0,
    "name_fuzzy":           25.0,
    "former_name_support":  10.0,
    "domain_support":        8.0,
    "address_support":       7.0,
    "country_support":       5.0,
    "parent_chain_support":  10.0,
    "identifier_support":    15.0,
    "acronym_penalty":       -5.0,
    "conflict_penalty":     -20.0,
    "ambiguity_penalty":    -10.0,
}

DEFAULT_HTTP_HEADERS = {
    "User-Agent": os.environ.get(
        "EDGAR_USER_AGENT",
        "SAMgovArby/2.0 (research; contact@example.com)"
    ),
    "Accept": "application/json",
}

PRIMARY_HISTORICAL_FORMS   = ["10-K", "10-Q", "20-F", "40-F"]
SECONDARY_HISTORICAL_FORMS = ["8-K"]

NON_PUBLIC_PATTERNS = [
    r"\bUNIVERSIT", r"\bREGENTS\b", r"\bTRUSTEES\b", r"\bBOARD OF\b",
    r"\bNATIONAL LABORATOR", r"\bDEPARTMENT OF\b", r"\bBUREAU OF\b",
    r"\bFOUNDATION\b", r"\bINSTITUTE OF\b", r"\bAUTHORIT[YI]",
    r"\bTRIBAL\b", r"\bCOUNTY OF\b", r"\bCITY OF\b", r"\bSTATE OF\b",
    r"\bCOMMISSION\b", r"\bGOVERNMENT\b", r"\bMUNICIPAL",
    r"\bCOOPERATIVE\b", r"\bASSOCIATION OF\b", r"\bCONSORTIUM\b",
    r"\bJOINT VENTURE\b", r"\b[A-Z]+ JV\b", r"\bAJV\b",
    r"\bBATTELLE\b", r"\bSANDIA\b", r"\bBROOKHAVEN\b", r"\bFERMILAB\b",
]

NOT_COMPETED_CODES = {"B", "C", "G", "CDO", "URG", "SP2"}

# Known company aliases: renames + acquisitions not in EDGAR map.
# Keys must be normalized (uppercase, no punctuation) stripped names.
# Covers: (1) EDGAR cache wrong-ticker entries, (2) post-acquisition renames,
# (3) subsidiaries commonly appearing in SAM.gov by a different name than the parent SEC filer.
KNOWN_ALIASES: dict[str, str] = {
    # ── Raytheon / RTX family ──────────────────────────────────────────────────
    "RAYTHEON":                           "RTX",
    "RAYTHEON BBN":                       "RTX",
    "RAYTHEON INTELLIGENCE AND SPACE":    "RTX",
    "RAYTHEON MISSILES AND DEFENSE":      "RTX",
    "UNITED TECHNOLOGIES":                "RTX",
    "COLLINS AEROSPACE":                  "RTX",
    "PRATT AND WHITNEY":                  "RTX",

    # ── L3Harris family ───────────────────────────────────────────────────────
    "HARRIS CORPORATION":                 "LHX",
    "L3 COMMUNICATIONS":                  "LHX",
    "L3HARRIS":                           "LHX",
    "L3HARRIS TECHNOLOGIES":              "LHX",
    "L3 TECHNOLOGIES":                    "LHX",

    # ── SAIC family ───────────────────────────────────────────────────────────
    "ENGILITY":                           "SAIC",
    "SCIENCE APPLICATIONS INTERNATIONAL": "SAIC",

    # ── V2X family ────────────────────────────────────────────────────────────
    "VECTRUS":                            "V2X",
    "VERTEX AEROSPACE":                   "V2X",

    # ── Boeing — EDGAR cache has wrong ticker BA-PA (preferred share) ─────────
    "BOEING":                             "BA",
    "BOEING COMPANY":                     "BA",
    "BOEING DEFENSE SPACE AND SECURITY":  "BA",
    "BOEING INTELLIGENCE AND ANALYTICS":  "BA",

    # ── Jacobs — renamed from Jacobs Engineering Group to Jacobs Solutions ────
    "JACOBS ENGINEERING GROUP":           "J",
    "JACOBS ENGINEERING":                 "J",
    "JACOBS TECHNOLOGY":                  "J",
    "JACOBS GOVERNMENT SERVICES":         "J",

    # ── Leidos family ─────────────────────────────────────────────────────────
    "LEIDOS":                             "LDOS",
    "LEIDOS INNOVATIONS":                 "LDOS",
    "LEIDOS HEALTH":                      "LDOS",
    "LEIDOS HOLDINGS":                    "LDOS",
    "SCIENCE AND TECHNOLOGY ASSOCIATES":  "LDOS",  # legacy Leidos entity

    # ── ManTech — was public (MANT) through late 2022 ────────────────────────
    "MANTECH":                            "MANT",
    "MANTECH INTERNATIONAL":              "MANT",
    "MANTECH ADVANCED SYSTEMS":           "MANT",

    # ── ICF International ─────────────────────────────────────────────────────
    "ICF INTERNATIONAL":                  "ICFI",
    "ICF INCORPORATED":                   "ICFI",

    # ── Tetra Tech ────────────────────────────────────────────────────────────
    "TETRA TECH":                         "TTEK",

    # ── Amentum (spun off from AECOM in 2020, IPO 2024) ──────────────────────
    "AMENTUM":                            "AMTM",
    "AMENTUM SERVICES":                   "AMTM",

    # ── DXC Technology (CSC + HP Enterprise Services merger 2017) ────────────
    "DXC TECHNOLOGY":                     "DXC",
    "COMPUTER SCIENCES CORPORATION":      "DXC",
    "CSC GOVERNMENT SOLUTIONS":           "DXC",

    # ── Peraton / Perspecta — Perspecta (PRSP) was public until May 2021 ─────
    "PERSPECTA":                          "PRSP",
    "PERSPECTA ENTERPRISE SOLUTIONS":     "PRSP",
    "PERSPECTA LABS":                     "PRSP",
}

# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class PathsConfig:
    base_dir:     str = "data"
    raw_dir:      str = "data/raw"
    staging_dir:  str = "data/staging"
    curated_dir:  str = "data/curated"
    index_dir:    str = "data/indexes"
    cache_dir:    str = "data/cache"
    output_dir:   str = "data/outputs"
    manifest_dir: str = "data/manifests"
    sqlite_path:  str = "data/cache/resolver.sqlite"
    duckdb_path:  str = "data/cache/resolver.duckdb"

@dataclass
class RuntimeConfig:
    max_workers:          int   = 4
    http_timeout_seconds: int   = 30
    retry_attempts:       int   = 3
    user_agent:           str   = DEFAULT_HTTP_HEADERS["User-Agent"]
    log_level:            str   = "INFO"
    memory_limit_gb:      float = 4.0

@dataclass
class ThresholdsConfig:
    entity_resolve_min_score:  float = 40.0
    entity_resolve_gap_min:    float = 10.0
    historical_symbol_min_score: float = 30.0
    final_resolve_min_score:   float = 35.0
    domain_support_bonus:      float = 8.0
    address_support_bonus:     float = 7.0
    former_name_penalty:       float = -5.0
    acronym_only_penalty:      float = -5.0
    null_if_conflict_penalty:  float = -20.0

@dataclass
class SourcePoliciesConfig:
    use_sam_monthly_extract:   bool = False
    allow_live_openfigi:       bool = True
    allow_gleif_api_fallback:  bool = False
    allow_sec_live_fallback:   bool = True
    sec_refresh_enabled:       bool = True
    gleif_refresh_enabled:     bool = False   # large files; opt-in
    otc_allowed:               bool = False
    adr_allowed:               bool = True

@dataclass
class HistoricalConfig:
    enable_award_date_symbol_resolution: bool       = True
    prefer_nearest_prior_filing:         bool       = True
    max_days_back_for_primary_lookup:    int        = 365
    max_days_back_for_fallback_lookup:   int        = 730
    pre_2019_auto_resolve_allowed:       bool       = False
    primary_forms:                       list[str]  = field(default_factory=lambda: PRIMARY_HISTORICAL_FORMS)
    secondary_forms:                     list[str]  = field(default_factory=lambda: SECONDARY_HISTORICAL_FORMS)

@dataclass
class SecuritySelectionConfig:
    prefer_common_stock:       bool      = True
    allow_adr_fallback:        bool      = True
    exclude_otc_default:       bool      = True
    us_exchange_whitelist:     set[str]  = field(default_factory=lambda: set(US_EXCHANGE_CODES_ALLOWED))
    disallowed_security_types: set[str]  = field(default_factory=lambda: set(DISALLOWED_SECURITY_TYPES))

@dataclass
class ResolverConfig:
    paths:              PathsConfig             = field(default_factory=PathsConfig)
    runtime:            RuntimeConfig           = field(default_factory=RuntimeConfig)
    thresholds:         ThresholdsConfig        = field(default_factory=ThresholdsConfig)
    source_policies:    SourcePoliciesConfig    = field(default_factory=SourcePoliciesConfig)
    historical:         HistoricalConfig        = field(default_factory=HistoricalConfig)
    security_selection: SecuritySelectionConfig = field(default_factory=SecuritySelectionConfig)

    def config_hash(self) -> str:
        """Stable hash of threshold + policy settings for cache invalidation."""
        key = str(self.thresholds) + str(self.source_policies) + str(self.security_selection)
        return hashlib.md5(key.encode()).hexdigest()[:12]

def load_config(config: ResolverConfig | dict | None = None) -> ResolverConfig:
    if config is None:
        return ResolverConfig()
    if isinstance(config, ResolverConfig):
        return config
    if isinstance(config, dict):
        cfg = ResolverConfig()
        for section, values in config.items():
            if hasattr(cfg, section) and isinstance(values, dict):
                section_obj = getattr(cfg, section)
                for k, v in values.items():
                    if hasattr(section_obj, k):
                        setattr(section_obj, k, v)
        return cfg
    raise ConfigError(f"Unsupported config type: {type(config)}")

def get_default_config() -> ResolverConfig:
    return ResolverConfig()

# ── Typed models ──────────────────────────────────────────────────────────────

@dataclass
class ContractRowCanonical:
    contract_row_id:                     str
    modification_number:                 str | None = None
    piid:                                str | None = None
    date_signed:                         date | None = None
    fiscal_year:                         int | None = None
    dollars_obligated:                   Decimal | None = None
    base_and_all_options_value:          Decimal | None = None
    cage_code:                           str | None = None
    contractor_name:                     str | None = None
    legal_business_name:                 str | None = None
    doing_business_as_name:              str | None = None
    ultimate_parent_legal_business_name: str | None = None
    unique_entity_id:                    str | None = None
    ultimate_parent_unique_entity_id:    str | None = None
    website_url:                         str | None = None
    vendor_address_city:                 str | None = None
    vendor_address_state:                str | None = None
    vendor_address_zip:                  str | None = None
    vendor_address_country:              str | None = None
    vendor_phone_number:                 str | None = None
    country_of_incorporation:            str | None = None
    naics_code:                          str | None = None
    naics_description:                   str | None = None
    product_or_service_code:             str | None = None
    product_or_service_description:      str | None = None
    entity_data_source:                  str | None = None
    # Sole-source flags (from existing codebase)
    extent_competed_code:                str | None = None
    other_than_full_open:                str | None = None
    num_offers:                          str | None = None
    is_educational_institution:          bool = False
    is_federal_agency:                   bool = False
    is_airport_authority:                bool = False
    is_council_of_governments:           bool = False
    is_community_dev_corp:               bool = False
    is_federally_funded_rd:              bool = False

@dataclass
class ContractIdentityFeatures:
    contract_row_id:            str
    award_date:                 date | None
    awardee_uei:                str | None
    parent_uei:                 str | None
    cage_code:                  str | None
    awardee_name_raw:           str | None
    awardee_name_norm:          str | None
    awardee_dba_raw:            str | None
    awardee_dba_norm:           str | None
    parent_name_raw:            str | None
    parent_name_norm:           str | None
    website_raw:                str | None
    website_domain:             str | None
    vendor_city_norm:           str | None
    vendor_state_norm:          str | None
    vendor_zip_norm:            str | None
    vendor_country_norm:        str | None
    incorporation_country_norm: str | None
    phone_norm:                 str | None
    dollars_obligated:          Decimal | None

@dataclass
class AwardeeEntity:
    entity_key:          str
    source_key_type:     Literal["uei", "cage", "synthetic"]
    uei:                 str | None
    cage_code:           str | None
    canonical_name:      str | None
    canonical_name_norm: str | None
    alias_names:         list[str] = field(default_factory=list)
    domains:             list[str] = field(default_factory=list)
    addresses:           list[dict] = field(default_factory=list)
    linked_parent_keys:  list[str] = field(default_factory=list)
    first_seen_date:     date | None = None
    last_seen_date:      date | None = None
    contract_count:      int = 0
    total_obligated:     Decimal | None = None

@dataclass
class ParentEntity:
    entity_key:          str
    source_key_type:     Literal["parent_uei", "synthetic"]
    parent_uei:          str | None
    canonical_name:      str | None
    canonical_name_norm: str | None
    alias_names:         list[str] = field(default_factory=list)
    domains:             list[str] = field(default_factory=list)
    countries:           list[str] = field(default_factory=list)
    linked_awardee_keys: list[str] = field(default_factory=list)
    first_seen_date:     date | None = None
    last_seen_date:      date | None = None
    contract_count:      int = 0
    total_obligated:     Decimal | None = None

@dataclass
class IssuerCandidate:
    candidate_id:        str
    source:              str   # MatchSource value
    issuer_key:          str
    issuer_name:         str
    issuer_name_norm:    str
    cik:                 str | None
    lei:                 str | None
    figi:                str | None
    match_level:         str
    entity_level:        Literal["awardee", "parent"]
    score_total:         float
    score_components:    dict[str, float] = field(default_factory=dict)
    supporting_evidence: dict[str, Any]  = field(default_factory=dict)
    conflicts:           list[str]        = field(default_factory=list)

@dataclass
class EntityResolutionDecision:
    entity_key:           str
    entity_type:          str  # EntityType value
    decision_status:      Literal["resolved_issuer", "ambiguous", "private", "no_match"]
    matched_issuer_key:   str | None
    matched_issuer_name:  str | None
    matched_cik:          str | None
    matched_lei:          str | None
    top_score:            float = 0.0
    second_score:         float | None = None
    score_gap:            float | None = None
    resolution_path:      str = ""
    evidence_json:        dict = field(default_factory=dict)

@dataclass
class HistoricalSymbolDecision:
    status:       str  # "resolved_symbol" | "historical_symbol_unavailable" | etc.
    symbol:       str | None = None
    exchange:     str | None = None
    source:       str | None = None
    score:        float | None = None
    evidence_json: dict = field(default_factory=dict)

@dataclass
class SecuritySelectionDecision:
    status:                str  # "resolved_security" | "no_us_tradable_security" | etc.
    selected_ticker:       str | None = None
    selected_exchange:     str | None = None
    selected_security_type: str | None = None
    is_adr:                bool | None = None
    source:                str | None = None
    score:                 float | None = None
    evidence_json:         dict = field(default_factory=dict)

@dataclass
class FinalResolution:
    contract_row_id:               str
    resolver_status:               str
    resolver_ticker:               str | None
    resolver_exchange:             str | None
    resolver_security_type:        str | None
    resolver_is_adr:               bool | None
    resolver_confidence:           str
    resolver_resolution_id:        str
    resolver_entity_key:           str | None
    resolver_parent_entity_key:    str | None
    resolver_issuer_key:           str | None
    resolver_award_date_used:      str | None
    # Decision trace
    resolver_resolution_path:      str | None = None
    resolver_entity_level_used:    str | None = None
    resolver_entity_match_source:  str | None = None
    resolver_historical_symbol_source: str | None = None
    resolver_security_selection_source: str | None = None
    resolver_top_candidate_score:  float | None = None
    resolver_second_candidate_score: float | None = None
    resolver_candidate_gap:        float | None = None
    resolver_manual_override_applied: bool = False
    resolver_null_reason:          str | None = None
    # Diagnostics
    resolver_awardee_name_norm:    str | None = None
    resolver_parent_name_norm:     str | None = None
    resolver_domain_norm:          str | None = None
    resolver_matched_issuer_name:  str | None = None
    resolver_matched_cik:          str | None = None
    resolver_matched_lei:          str | None = None
    resolver_matched_figi:         str | None = None
    resolver_score_breakdown_json: str | None = None
    resolver_evidence_json:        str | None = None
    resolver_version:              str = "2.0"

@dataclass
class RefreshReport:
    source_name: str
    started_at:  datetime
    ended_at:    datetime
    status:      str   # "ok" | "failed" | "skipped"
    rows_written: int = 0
    details:     dict = field(default_factory=dict)
    error:       str | None = None

@dataclass
class ExplanationRecord:
    resolution_id:            str
    awardee_name_norm:        str | None
    parent_name_norm:         str | None
    domain_norm:              str | None
    candidates_considered:    list[dict] = field(default_factory=list)
    top_scores:               list[float] = field(default_factory=list)
    chosen_path:              str | None = None
    null_reason:              str | None = None
    historical_ticker_evidence: dict = field(default_factory=dict)
    security_selection_evidence: dict = field(default_factory=dict)

@dataclass
class OverrideRecord:
    entity_key:     str
    fixed_issuer:   str | None = None
    fixed_ticker:   str | None = None
    forced_null:    bool = False
    award_date_from: date | None = None
    award_date_to:   date | None = None
    reason:         str | None = None
    reviewer:       str | None = None
    created_at:     datetime = field(default_factory=datetime.utcnow)

@dataclass
class ReferenceHandles:
    """Loaded reference tables passed around instead of re-reading from disk."""
    edgar_map:           dict = field(default_factory=dict)  # name -> {ticker, cik}
    sec_issuer_master:   dict = field(default_factory=dict)  # cik -> {name, tickers, exchanges, formerNames, sic}
    sec_alias_table:     dict = field(default_factory=dict)  # name_norm -> [cik, ...]
    gleif_entity_master: dict = field(default_factory=dict)  # lei -> entity
    gleif_alias_table:   dict = field(default_factory=dict)  # name_norm -> [lei, ...]
    gleif_relationships: dict = field(default_factory=dict)  # lei -> {direct_parent_lei, ultimate_parent_lei}
    isin_lei_map:        dict = field(default_factory=dict)  # isin -> lei
    security_cache:      dict = field(default_factory=dict)   # ticker -> security metadata
    substr_index:        list = field(default_factory=list)   # [(stripped, name, entry), ...] for substring match
    loaded_at:           datetime = field(default_factory=datetime.utcnow)
    reference_version:   str = "unknown"
