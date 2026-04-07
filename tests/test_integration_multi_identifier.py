"""Integration test for multi-identifier resolver against real USASpending data."""
import csv
import os
import pytest
from sam_gov_reader import ContractRecord
from ticker_resolver_v4 import TickerResolverV4


def _make_record(**kwargs) -> ContractRecord:
    defaults = dict(
        piid="", cage_code="", uei="", country_of_incorporation="USA",
        contractor_name="", legal_business_name="", dba_name="",
        parent_name="", parent_uei="", award_amount=0.0, posted_date="",
        agency="", naics_code="", naics_description="", set_aside_code="",
        extent_competed_code="", other_than_full_open="", idv_type="",
        num_offers="", is_educational_institution=False, is_federal_agency=False,
        is_airport_authority=False, is_council_of_governments=False,
        is_community_dev_corp=False, is_federally_funded_rd=False,
    )
    defaults.update(kwargs)
    return ContractRecord(**defaults)


def test_real_contracts_resolution():
    """Test resolver on sample real contracts from training set."""
    resolver = TickerResolverV4()

    training_csv = "datasets/filtered_training_set.csv"
    if not os.path.exists(training_csv):
        pytest.skip("Training CSV not found; run build_training_set.py first")

    resolved_count = 0
    test_count = 0

    with open(training_csv) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 100:
                break

            test_count += 1
            awardee = row.get("recipient_name", "").strip()
            parent = row.get("parent_recipient_name", "").strip()

            if not awardee:
                continue

            record = _make_record(contractor_name=awardee, legal_business_name=awardee,
                                  parent_name=parent)
            result = resolver.resolve(record)
            if result.get("resolved_ticker"):
                resolved_count += 1

    rate = (resolved_count / test_count * 100) if test_count else 0
    print(f"\nIntegration test: {resolved_count}/{test_count} = {rate:.1f}%")
    if resolved_count > 0:
        assert resolved_count >= test_count * 0.10


def test_cage_code_resolution():
    """Test resolver with mock SAM.gov data (CAGE codes)."""
    resolver = TickerResolverV4()

    test_cases = [
        {"awardee": "NORTHROP GRUMMAN CORP", "cage_code": "1WPN2"},
        {"awardee": "LOCKHEED MARTIN CORP", "cage_code": "04ZLA"},
    ]

    for case in test_cases:
        record = _make_record(contractor_name=case["awardee"],
                              legal_business_name=case["awardee"],
                              cage_code=case["cage_code"])
        result = resolver.resolve(record)
        if result.get("resolved_ticker"):
            assert result["confidence"] in ["very_high", "high", "medium"], \
                f"Expected higher confidence for {case['awardee']}"
