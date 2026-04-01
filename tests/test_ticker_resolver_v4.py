"""Unit tests for TickerResolverV4."""
import pytest
from unittest.mock import MagicMock, patch
from sam_gov_reader import ContractRecord
from ticker_resolver_v4 import TickerResolverV4


# ─── Factory ─────────────────────────────────────────────────────────────────

def make_record(**overrides) -> ContractRecord:
    defaults = dict(
        piid="W911NF23C1001",
        cage_code="1RBX4",
        uei="ABCDEF123456",
        country_of_incorporation="USA",
        contractor_name="ACME DEFENSE INC",
        legal_business_name="ACME DEFENSE INC",
        dba_name="",
        parent_name="ACME CORP",
        parent_uei="PARENT123456",
        award_amount=5_000_000.0,
        posted_date="2023-03-15",
        agency="DEPT OF DEFENSE",
        naics_code="336411",
        naics_description="Aircraft Manufacturing",
        set_aside_code="",
        extent_competed_code="A",
        other_than_full_open="",
        idv_type="",
        num_offers="3",
        is_educational_institution=False,
        is_federal_agency=False,
        is_airport_authority=False,
        is_council_of_governments=False,
        is_community_dev_corp=False,
        is_federally_funded_rd=False,
    )
    defaults.update(overrides)
    return ContractRecord(**defaults)


def make_resolver(edgar_map: dict | None = None) -> TickerResolverV4:
    """Create resolver with minimal EDGAR map (no network calls)."""
    return TickerResolverV4(edgar_map=edgar_map or {}, cache_path=":memory:")


# ─── Tier 0 tests ─────────────────────────────────────────────────────────────

def test_tier0_educational_institution_rejected():
    r = make_resolver()
    result = r.resolve(make_record(is_educational_institution=True))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_federal_agency_rejected():
    r = make_resolver()
    result = r.resolve(make_record(is_federal_agency=True))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_airport_authority_rejected():
    r = make_resolver()
    result = r.resolve(make_record(is_airport_authority=True))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_council_of_governments_rejected():
    r = make_resolver()
    result = r.resolve(make_record(is_council_of_governments=True))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_community_dev_corp_rejected():
    r = make_resolver()
    result = r.resolve(make_record(is_community_dev_corp=True))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_federally_funded_rd_rejected():
    r = make_resolver()
    result = r.resolve(make_record(is_federally_funded_rd=True))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_name_regex_university_rejected():
    r = make_resolver()
    result = r.resolve(make_record(
        cage_code="",
        contractor_name="UNIVERSITY OF MICHIGAN",
        legal_business_name="UNIVERSITY OF MICHIGAN",
    ))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_name_regex_county_rejected():
    r = make_resolver()
    result = r.resolve(make_record(
        cage_code="",
        contractor_name="COUNTY OF MONTGOMERY",
        legal_business_name="COUNTY OF MONTGOMERY",
    ))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_foreign_country_rejected():
    r = make_resolver()
    result = r.resolve(make_record(country_of_incorporation="GBR"))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "non_public_entity"


def test_tier0_clean_record_not_rejected():
    r = make_resolver()
    result = r.resolve(make_record(cage_code=""))
    # Should fall through to unresolved (empty EDGAR map), NOT non_public_entity
    assert result["rejection_reason"] != "non_public_entity"


# ─── Tier 1 tests ─────────────────────────────────────────────────────────────

def test_tier1_cage_resolves_to_ticker():
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {
        "lei": "ABCDE12345FGHIJ67890",
        "confidence": 0.95,
        "rejection_reason": None,
        "source": "gleif",
    }
    lei_mock = MagicMock()
    lei_mock.resolve_lei.return_value = {
        "ticker": "ACME",
        "cik": "0001234567",
        "confidence": "high",
    }
    r = make_resolver()
    r.cage_resolver = cage_mock
    r.lei_resolver = lei_mock
    r.mcap_cache["ACME"] = 500_000_000.0

    result = r.resolve(make_record(cage_code="1RBX4"))

    assert result["resolved_ticker"] == "ACME"
    assert result["confidence"] == "high"
    assert result["evidence_type"] == "cage_lei_openfigi"
    cage_mock.resolve_cage.assert_called_once_with("1RBX4")
    lei_mock.resolve_lei.assert_called_once_with("ABCDE12345FGHIJ67890")


def test_tier1_cage_gleif_miss_falls_through():
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {
        "lei": None, "confidence": 0,
        "rejection_reason": "not_found_in_gleif", "source": "none",
    }
    r = make_resolver()
    r.cage_resolver = cage_mock
    result = r.resolve(make_record(cage_code="1RBX4", contractor_name="XYZZY NO MATCH CO",
                                   legal_business_name="XYZZY NO MATCH CO"))
    assert result["resolved_ticker"] is None
    assert result["evidence_type"] == "unresolved"


def test_tier1_empty_cage_skips_to_tier2():
    cage_mock = MagicMock()
    r = make_resolver()
    r.cage_resolver = cage_mock
    r.resolve(make_record(cage_code=""))
    cage_mock.resolve_cage.assert_not_called()


# ─── Tier 2 tests ─────────────────────────────────────────────────────────────

def test_tier2_exact_legal_name_match():
    edgar_map = {"LOCKHEED MARTIN CORPORATION": {"ticker": "LMT", "cik": "0000936468"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["LMT"] = 100_000_000_000.0

    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(make_record(
            cage_code="",
            legal_business_name="LOCKHEED MARTIN CORPORATION",
            contractor_name="LOCKHEED MARTIN",
        ))

    assert result["resolved_ticker"] == "LMT"
    assert result["confidence"] == "high"


def test_tier2_contractor_name_fallback():
    edgar_map = {"NORTHROP GRUMMAN CORPORATION": {"ticker": "NOC", "cik": "0001133421"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["NOC"] = 50_000_000_000.0

    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(make_record(
            cage_code="",
            legal_business_name="NORTHROP GRUMMAN SYSTEMS CORP",
            contractor_name="NORTHROP GRUMMAN CORPORATION",
        ))

    assert result["resolved_ticker"] == "NOC"


def test_tier2_dba_name_fallback():
    edgar_map = {"GENERAL DYNAMICS CORPORATION": {"ticker": "GD", "cik": "0000040533"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["GD"] = 60_000_000_000.0

    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(make_record(
            cage_code="",
            legal_business_name="GD ADVANCED INFORMATION SYSTEMS LLC",
            contractor_name="GD AIS LLC",
            dba_name="GENERAL DYNAMICS CORPORATION",
        ))

    assert result["resolved_ticker"] == "GD"


def test_tier2_parent_name_fallback():
    edgar_map = {"RAYTHEON TECHNOLOGIES CORPORATION": {"ticker": "RTX", "cik": "0000101829"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["RTX"] = 130_000_000_000.0

    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(make_record(
            cage_code="",
            legal_business_name="RAYTHEON INTELLIGENCE AND SPACE LLC",
            contractor_name="RTX MISSION SYSTEMS",
            dba_name="",
            parent_name="RAYTHEON TECHNOLOGIES CORPORATION",
        ))

    assert result["resolved_ticker"] == "RTX"


# ─── Tier 3 tests ─────────────────────────────────────────────────────────────

def test_tier3_fuzzy_match_resolves():
    # "NORTHOP GRUMMAN" is a one-char typo — fails exact/stripped match but
    # scores ~94% on token_sort_ratio against "NORTHROP GRUMMAN" (above threshold 85).
    edgar_map = {"NORTHROP GRUMMAN": {"ticker": "NOC", "cik": "0001133421"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["NOC"] = 50_000_000_000.0

    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(True, "high", "exact_sec_name")):
        result = r.resolve(make_record(
            cage_code="",
            legal_business_name="NORTHOP GRUMMAN",   # typo: missing R
            contractor_name="NORTHOP GRUMMAN",
            parent_name="",
        ))

    assert result["resolved_ticker"] == "NOC"
    assert "fuzzy" in result["evidence_type"]


# ─── Tier 4 tests ─────────────────────────────────────────────────────────────

def test_tier4_subsidiary_substring_match():
    edgar_map = {"NORTHROP GRUMMAN": {"ticker": "NOC", "cik": "0001133421"}}
    r = make_resolver(edgar_map)
    r.mcap_cache["NOC"] = 50_000_000_000.0

    with patch("ticker_resolver_v4._validate_candidate",
               return_value=(False, "none", "name_mismatch")):
        result = r.resolve(make_record(
            cage_code="",
            legal_business_name="NORTHROP GRUMMAN SYSTEMS CORPORATION",
            contractor_name="NORTHROP GRUMMAN SYSTEMS CORPORATION",
        ))

    assert result["resolved_ticker"] == "NOC"
    assert result["evidence_type"] == "substring_match"


# ─── Tier 5 tests ─────────────────────────────────────────────────────────────

def test_tier5_sole_source_tagged_when_num_offers_is_1():
    r = make_resolver()
    result = r.resolve(make_record(cage_code="", num_offers="1"))
    assert result["resolved_ticker"] is None
    assert result["rejection_reason"] == "sole_source_unresolved"


def test_tier5_not_competed_code_tagged():
    r = make_resolver()
    result = r.resolve(make_record(cage_code="", extent_competed_code="B", num_offers="1"))
    assert result["rejection_reason"] == "sole_source_unresolved"


def test_tier5_normal_unresolved_no_match():
    r = make_resolver()
    result = r.resolve(make_record(cage_code="", num_offers="5"))
    assert result["rejection_reason"] == "no_match"


# ─── Cache key tests ──────────────────────────────────────────────────────────

def test_cache_key_prefers_cage_over_name():
    r = make_resolver()
    record1 = make_record(cage_code="1RBX4", contractor_name="ACME INC")
    record2 = make_record(cage_code="1RBX4", contractor_name="ACME DEFENSE INC")
    result1 = r.resolve(record1)
    result2 = r.resolve(record2)
    assert result1 is result2


def test_cache_key_uses_uei_when_no_cage():
    r = make_resolver()
    record1 = make_record(cage_code="", uei="ABCDEF123456", contractor_name="ACME INC")
    record2 = make_record(cage_code="", uei="ABCDEF123456", contractor_name="ACME DEFENSE INC")
    result1 = r.resolve(record1)
    result2 = r.resolve(record2)
    assert result1 is result2
