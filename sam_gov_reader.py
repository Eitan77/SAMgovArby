"""SAM.gov bulk CSV reader — yields typed ContractRecord dataclasses.

SAM.gov report CSVs have a 5-line preamble (report title, blank, filter
description, filter value, blank) before the actual column headers. This
reader detects the header row automatically by scanning for the line that
contains "CAGE Code".

Filters applied at read time:
- Hard-reject: Country of Incorporation != "USA"
- Hard-reject: IDV Type non-empty (umbrella contract vehicles)
- Hard-reject: award amount outside $1M–$10B range or unparseable
"""
from __future__ import annotations

import csv
import glob
import os
from dataclasses import dataclass
from typing import Iterator

from config import MIN_CONTRACT_VALUE, MAX_AWARD_AMOUNT

_AMOUNT_COL = "Base and All Options Value (Total Contract Value)"

_FLAG_COLS = {
    "is_educational_institution": "Is Vendor Business Type - Educational Institution",
    "is_federal_agency":          "Is Vendor Business Type - Federal Agency",
    "is_airport_authority":       "Is Vendor Business Type - Airport Authority",
    "is_council_of_governments":  "Is Vendor Business Type - Council Of Governments",
    "is_community_dev_corp":      "Is Vendor Business Type - Community Development Corporation",
    "is_federally_funded_rd":     "Is Vendor Business Type - Federally Funded Research and Development Corporation",
}


@dataclass
class ContractRecord:
    # Identity
    piid: str
    cage_code: str
    uei: str
    country_of_incorporation: str

    # Names (priority order for resolution: legal → contractor → dba → parent)
    contractor_name: str
    legal_business_name: str
    dba_name: str
    parent_name: str
    parent_uei: str

    # Contract fields
    award_amount: float
    posted_date: str
    agency: str
    naics_code: str
    naics_description: str
    set_aside_code: str
    extent_competed_code: str
    other_than_full_open: str
    idv_type: str
    num_offers: str

    # Non-public detection flags ("Yes"/"No" → bool)
    is_educational_institution: bool
    is_federal_agency: bool
    is_airport_authority: bool
    is_council_of_governments: bool
    is_community_dev_corp: bool
    is_federally_funded_rd: bool


def read_sam_gov_csv(path: str) -> Iterator[ContractRecord]:
    """Read SAM.gov bulk CSV export, yield validated ContractRecord per row.

    Handles the SAM.gov report preamble by scanning for the header row
    (the line containing "CAGE Code").

    Silently skips: foreign entities, IDV umbrellas, out-of-range/unparseable amounts.
    Raises: FileNotFoundError if path does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"SAM.gov CSV not found: {path}")

    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        # Skip preamble: advance until we find the header row (contains "CAGE Code")
        header_line = None
        for raw_line in f:
            if "CAGE Code" in raw_line:
                header_line = raw_line
                break

        if header_line is None:
            return  # No header found — empty or malformed file

        # Re-parse from this point using csv.DictReader with the detected header
        import io
        remaining = header_line + f.read()
        reader = csv.DictReader(io.StringIO(remaining))

        for row in reader:
            # Hard-reject: foreign / missing country
            country = (row.get("Country of Incorporation") or "").strip().upper()
            if country != "USA":
                continue

            # Hard-reject: IDV umbrella contracts
            if (row.get("IDV Type") or "").strip():
                continue

            # Hard-reject: unparseable or out-of-range amount
            try:
                amount = float((row.get(_AMOUNT_COL) or "0").replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                continue
            if amount < MIN_CONTRACT_VALUE or amount > MAX_AWARD_AMOUNT:
                continue

            yield ContractRecord(
                piid=                    (row.get("PIID") or "").strip(),
                cage_code=               (row.get("CAGE Code") or "").strip(),
                uei=                     (row.get("Unique Entity ID") or "").strip(),
                country_of_incorporation=country,
                contractor_name=         (row.get("Contractor Name") or "").strip(),
                legal_business_name=     (row.get("Legal Business Name") or "").strip(),
                dba_name=                (row.get("Doing Business As Name") or "").strip(),
                parent_name=             (row.get("Ultimate Parent Legal Business Name") or "").strip(),
                parent_uei=              (row.get("Ultimate Parent Unique Entity ID") or "").strip(),
                award_amount=            amount,
                posted_date=             _parse_date(row.get("Period of Performance Start Date") or ""),
                agency=                  (row.get("Contracting Agency Name") or "").strip(),
                naics_code=              (row.get("NAICS Code") or "").strip(),
                naics_description=       (row.get("NAICS Description") or "").strip(),
                set_aside_code=          (row.get("Type of Set Aside Code") or "").strip(),
                extent_competed_code=    (row.get("Extent Competed Code") or "").strip(),
                other_than_full_open=    (row.get("Other Than Full and Open Competition Code") or "").strip(),
                idv_type=                (row.get("IDV Type") or "").strip(),
                num_offers=              (row.get("Number of Offers Received") or "").strip(),
                is_educational_institution= _yes(row, _FLAG_COLS["is_educational_institution"]),
                is_federal_agency=          _yes(row, _FLAG_COLS["is_federal_agency"]),
                is_airport_authority=       _yes(row, _FLAG_COLS["is_airport_authority"]),
                is_council_of_governments=  _yes(row, _FLAG_COLS["is_council_of_governments"]),
                is_community_dev_corp=      _yes(row, _FLAG_COLS["is_community_dev_corp"]),
                is_federally_funded_rd=     _yes(row, _FLAG_COLS["is_federally_funded_rd"]),
            )


def _yes(row: dict, col: str) -> bool:
    return (row.get(col) or "").strip().upper() == "YES"


def _parse_date(raw: str) -> str:
    """Return first 10 chars (YYYY-MM-DD) or empty string."""
    raw = raw.strip()
    return raw[:10] if len(raw) >= 10 else raw


def find_sam_gov_csv(dataset_dir: str) -> str:
    """Scan dataset_dir for SAM.gov bulk CSV exports, return path of most recent.

    Raises: RuntimeError if no matching CSV found.
    """
    patterns = [
        os.path.join(dataset_dir, "*Report*.csv"),
        os.path.join(dataset_dir, "*SAM*.csv"),
    ]
    matches = []
    for pat in patterns:
        matches.extend(glob.glob(pat))
    if not matches:
        raise RuntimeError(
            f"No SAM.gov CSV found in {dataset_dir}/\n"
            "Download a report from sam.gov and place it in the datasets/ directory."
        )
    return max(matches, key=os.path.getmtime)
