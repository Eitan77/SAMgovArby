"""Unit tests for sam_gov_reader.py"""
import csv
import os
import tempfile
import pytest
from sam_gov_reader import ContractRecord, read_sam_gov_csv


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_csv_file(rows: list[dict]) -> str:
    """Write rows to a temp CSV file (no preamble), return path."""
    if not rows:
        raise ValueError("rows must be non-empty")
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return f.name


def _make_csv_file_with_preamble(rows: list[dict]) -> str:
    """Write rows to a temp CSV file with SAM.gov-style preamble, return path."""
    if not rows:
        raise ValueError("rows must be non-empty")
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
    ) as f:
        # Mimic real SAM.gov report preamble
        f.write("FirstReport\r\n")
        f.write("\r\n")
        f.write("Report Filter:\r\n")
        f.write("{Date Signed} Between 1/1/2023 and 1/10/2023\r\n")
        f.write("\r\n")
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return f.name


def _base_row(**overrides) -> dict:
    """Return a minimal valid SAM.gov CSV row dict."""
    row = {
        "BLANK1 (DEPRECATED)": "",
        "BLANK2 (DEPRECATED)": "",
        "CAGE Code": "1RBX4",
        "Unique Entity ID": "ABCDEF123456",
        "Contractor Name": "ACME DEFENSE INC",
        "Legal Business Name": "ACME DEFENSE INC",
        "Doing Business As Name": "",
        "Ultimate Parent Legal Business Name": "ACME CORP",
        "Ultimate Parent Unique Entity ID": "PARENT123456",
        "Country of Incorporation": "USA",
        "Base and All Options Value (Total Contract Value)": "5000000.00",
        "Period of Performance Start Date": "2023-03-15",
        "Contracting Agency Name": "DEPT OF DEFENSE",
        "NAICS Code": "336411",
        "NAICS Description": "Aircraft Manufacturing",
        "Type of Set Aside Code": "",
        "Extent Competed Code": "A",
        "Other Than Full and Open Competition Code": "",
        "IDV Type": "",
        "Number of Offers Received": "3",
        "PIID": "W911NF23C1001",
        "Is Vendor Business Type - Educational Institution": "No",
        "Is Vendor Business Type - Federal Agency": "No",
        "Is Vendor Business Type - Airport Authority": "No",
        "Is Vendor Business Type - Council Of Governments": "No",
        "Is Vendor Business Type - Community Development Corporation": "No",
        "Is Vendor Business Type - Federally Funded Research and Development Corporation": "No",
    }
    row.update(overrides)
    return row


# ─── Tests ───────────────────────────────────────────────────────────────────

def test_valid_row_yields_contract_record():
    path = _make_csv_file([_base_row()])
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 1
        r = records[0]
        assert isinstance(r, ContractRecord)
        assert r.piid == "W911NF23C1001"
        assert r.cage_code == "1RBX4"
        assert r.uei == "ABCDEF123456"
        assert r.contractor_name == "ACME DEFENSE INC"
        assert r.legal_business_name == "ACME DEFENSE INC"
        assert r.dba_name == ""
        assert r.parent_name == "ACME CORP"
        assert r.parent_uei == "PARENT123456"
        assert r.country_of_incorporation == "USA"
        assert r.award_amount == 5_000_000.0
        assert r.posted_date == "2023-03-15"
        assert r.agency == "DEPT OF DEFENSE"
        assert r.naics_code == "336411"
        assert r.set_aside_code == ""
        assert r.extent_competed_code == "A"
        assert r.idv_type == ""
        assert r.num_offers == "3"
        assert r.is_educational_institution is False
        assert r.is_federal_agency is False
    finally:
        os.unlink(path)


def test_preamble_skipped_correctly():
    """SAM.gov report preamble (5 lines) is skipped, data rows are read."""
    path = _make_csv_file_with_preamble([_base_row()])
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 1
        assert records[0].piid == "W911NF23C1001"
    finally:
        os.unlink(path)


def test_foreign_entity_skipped():
    path = _make_csv_file([_base_row(**{"Country of Incorporation": "GBR"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_missing_country_skipped():
    path = _make_csv_file([_base_row(**{"Country of Incorporation": ""})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_idv_type_nonempty_skipped():
    path = _make_csv_file([_base_row(**{"IDV Type": "IDC"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_amount_below_min_skipped():
    path = _make_csv_file([_base_row(**{"Base and All Options Value (Total Contract Value)": "500000"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_amount_above_max_skipped():
    path = _make_csv_file([_base_row(**{"Base and All Options Value (Total Contract Value)": "15000000000"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_unparseable_amount_skipped():
    path = _make_csv_file([_base_row(**{"Base and All Options Value (Total Contract Value)": "N/A"})])
    try:
        records = list(read_sam_gov_csv(path))
        assert records == []
    finally:
        os.unlink(path)


def test_business_type_flags_yes_parsed_as_true():
    path = _make_csv_file([_base_row(**{
        "Is Vendor Business Type - Educational Institution": "Yes",
        "Is Vendor Business Type - Federal Agency": "Yes",
    })])
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 1
        assert records[0].is_educational_institution is True
        assert records[0].is_federal_agency is True
    finally:
        os.unlink(path)


def test_multiple_rows_mixed_filtering():
    rows = [
        _base_row(PIID="A001"),
        {**_base_row(PIID="A002"), "Country of Incorporation": "CAN"},
        {**_base_row(PIID="A003"), "IDV Type": "IDC"},
        _base_row(PIID="A004"),
    ]
    path = _make_csv_file(rows)
    try:
        records = list(read_sam_gov_csv(path))
        assert len(records) == 2
        assert records[0].piid == "A001"
        assert records[1].piid == "A004"
    finally:
        os.unlink(path)


def test_file_not_found_raises():
    with pytest.raises(FileNotFoundError):
        list(read_sam_gov_csv("nonexistent_file.csv"))
