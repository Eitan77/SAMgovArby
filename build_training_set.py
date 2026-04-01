"""Build a government contract arbitrage training dataset.

Pipeline (sequential, checkpoint-resumable):
  Stage 1 — Load & Filter    → datasets/filtered_training_set.csv
  Stage 2 — Ticker Resolve   → datasets/stage2_with_tickers.csv
  Stage 3 — Enrich           → datasets/training_set_final.csv
             (prices, shares, historical mcap, 8-K, dilutive filings)

Filters applied in Stage 1:
  - Keep $1M–$10B awards only
  - Remove top-20 companies by contract count (bulk spammers)
  - Remove all IDIQ contracts

Stages 2–3 only process rows that pass each prior gate (ticker resolved).
Checkpoints are saved in batches — interrupt and resume safely.

Prerequisites:
    Download the FY2023 contracts bulk file from:
      https://files.usaspending.gov/award_data_archive/
    Place the zip in datasets/

Usage:
    python build_training_set.py [--quiet] [--verbose] [--json]
"""

from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import logging
import os
import sys
import time
import warnings
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

warnings.filterwarnings("ignore", category=FutureWarning)

import polars as pl
import requests
import yfinance as yf

from config_logging import setup_logging, add_verbosity_flags
from config import (
    MIN_CONTRACT_VALUE, MAX_AWARD_AMOUNT, TOP_N_TO_REMOVE,
    EDGAR_RATE_LIMIT, EDGAR_8K_ENRICHMENT_DAYS, EDGAR_USER_AGENT,
)
from sam_gov_reader import read_sam_gov_csv, find_sam_gov_csv, ContractRecord

# ─── Logging (initialized in main, default for module-level usage) ─────────────

log = logging.getLogger("build")

# ─── Paths ────────────────────────────────────────────────────────────────────

ROOT = os.path.dirname(__file__)
DATASET_DIR = os.path.join(ROOT, "datasets")
CHECKPOINT_DIR = os.path.join(DATASET_DIR, "checkpoints")
TICKER_CACHE_V2_FILE = os.path.join(ROOT, ".ticker_cache_v2.json")
TICKER_CACHE_V4_FILE = os.path.join(ROOT, ".ticker_cache_v4.json")
EDGAR_MAP_FILE = os.path.join(ROOT, ".edgar_tickers.json")

FILTERED_CSV = os.path.join(DATASET_DIR, "filtered_training_set.csv")
STAGE2_CSV   = os.path.join(DATASET_DIR, "stage2_with_tickers.csv")
FINAL_CSV    = os.path.join(DATASET_DIR, "training_set_final.csv")

CP_STAGE1 = os.path.join(CHECKPOINT_DIR, "stage1_filter.json")
CP_STAGE2 = os.path.join(CHECKPOINT_DIR, "stage2_tickers.json")
CP_STAGE3 = os.path.join(CHECKPOINT_DIR, "stage3_enrich.json")

# ─── Local aliases (from config — single source of truth) ─────────────────────

EDGAR_RATE_LIMIT_SEC = EDGAR_RATE_LIMIT       # seconds between EDGAR requests
EDGAR_8K_WINDOW_DAYS = EDGAR_8K_ENRICHMENT_DAYS  # enrichment look-ahead window

IDIQ_INDICATORS = ["idiq", "indefinite delivery", "indefinite quantity"]
SOLE_SOURCE_INDICATORS = [
    "sole source", "sole-source", "only one source",
    "other than full", "8(a) sole",
]

EDGAR_TICKERS_URL    = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_HEADERS = {
    "User-Agent": EDGAR_USER_AGENT,
    "Accept": "application/json",
}

# ─── Shared utilities ─────────────────────────────────────────────────────────

_edgar_last = 0.0


def _edgar_throttle():
    global _edgar_last
    elapsed = time.time() - _edgar_last
    if elapsed < EDGAR_RATE_LIMIT_SEC:
        time.sleep(EDGAR_RATE_LIMIT_SEC - elapsed)
    _edgar_last = time.time()


def _elapsed(t0: float) -> str:
    secs = int(time.time() - t0)
    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"


def _load_cp(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Checkpoint load failed {path}: {e}")
    return {}


def _save_cp(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f)


def _write_csv(path: str, rows: list):
    if not rows:
        log.warning(f"No rows — skipping write: {path}")
        return
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen:
                keys.append(k)
                seen.add(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in keys})
    size_kb = os.path.getsize(path) / 1024
    log.info(f"  Wrote {len(rows):,} rows → {os.path.basename(path)} ({size_kb:.0f} KB)")


def _read_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — LOAD & FILTER
# ═══════════════════════════════════════════════════════════════════════════════

def _find_bulk_file(year: int) -> str:
    patterns = [
        os.path.join(DATASET_DIR, f"FY{year}_All_Contracts_Full_*.zip"),
        os.path.join(DATASET_DIR, f"FY{year}_All_Contracts_Full_*.csv"),
        os.path.join(DATASET_DIR, f"FY{year}*Contracts*.zip"),
        os.path.join(DATASET_DIR, f"FY{year}*Contract*.zip"),
        os.path.join(DATASET_DIR, f"FY{year}*Contract*.csv"),
    ]
    for pat in patterns:
        matches = sorted(glob.glob(pat))
        if matches:
            return matches[-1]
    return ""


def _parse_bulk_row(row: dict, month_filter: int = 0) -> tuple | None:
    """Parse one CSV row → (award_key, award_dict) or None to discard.

    Args:
        month_filter: Filter to specific month (1-12) or 0 for all months
    """
    try:
        # IDV = umbrella contract vehicle, not an individual award
        if (row.get("award_or_idv_flag") or "").upper() == "IDV":
            return None

        name = (row.get("recipient_name") or "").strip()
        amount_str = (
            row.get("current_total_value_of_award")
            or row.get("total_dollars_obligated")
            or row.get("federal_action_obligation")
            or "0"
        )
        amount = float(amount_str)

        if not name:
            return None
        if amount < MIN_CONTRACT_VALUE or amount > MAX_AWARD_AMOUNT:
            return None

        # Optional: filter by month (e.g., month_filter=3 for March)
        if month_filter:
            posted = row.get("period_of_performance_start_date") or row.get("action_date") or ""
            if posted:
                try:
                    month = int(posted.split("-")[1])
                    if month != month_filter:
                        return None
                except (ValueError, IndexError):
                    pass

        award_key = row.get("contract_award_unique_key") or ""
        if not award_key:
            return None

        set_aside       = (row.get("type_of_set_aside") or "").lower()
        contract_type   = (row.get("type_of_contract_pricing") or "").lower()
        extent_competed = (row.get("extent_competed") or "").lower()
        other_than_full = (row.get("other_than_full_and_open_competition") or "").lower()
        idv_type        = (row.get("idv_type") or "").lower()
        type_of_idc     = (row.get("type_of_idc") or "").lower()
        combined        = f"{set_aside} {contract_type} {extent_competed} {other_than_full} {idv_type} {type_of_idc}"

        sole_source = (
            any(i in combined for i in SOLE_SOURCE_INDICATORS)
            or extent_competed in ("not competed", "not available for competition")
        )
        is_idiq = any(i in combined for i in IDIQ_INDICATORS)

        posted = row.get("period_of_performance_start_date") or row.get("action_date") or ""

        return award_key, {
            "award_key":       award_key,
            "award_id":        row.get("award_id_piid") or award_key,
            "posted_date":     posted[:10] if posted else "",
            "awardee_name":    name,
            "award_amount":    amount,
            "agency":          row.get("awarding_agency_name") or "",
            "sub_agency":      row.get("awarding_sub_agency_name") or "",
            "naics":           str(row.get("naics_code") or ""),
            "naics_description": row.get("naics_description") or "",
            "set_aside":       row.get("type_of_set_aside") or "",
            "extent_competed": row.get("extent_competed") or "",
            "sole_source":     sole_source,
            "is_idiq":         is_idiq,
            "parent_recipient_name": (row.get("recipient_parent_name") or "").strip(),
        }
    except (ValueError, TypeError) as e:
        log.debug(f"Parse error: {e}")
        return None


def _record_to_award_dict(record: ContractRecord) -> dict:
    """Map a ContractRecord to the award dict format used by downstream stages."""
    sole_source = (
        record.num_offers == "1"
        or record.extent_competed_code in ("B", "C", "G", "CDO", "URG", "SP2")
        or (record.other_than_full_open or "").strip().upper() not in ("", "NO", "N")
    )
    return {
        "award_key":             record.piid,
        "award_id":              record.piid,
        "posted_date":           record.posted_date,
        "awardee_name":          record.contractor_name or record.legal_business_name,
        "legal_business_name":   record.legal_business_name,
        "dba_name":              record.dba_name,
        "cage_code":             record.cage_code,
        "uei":                   record.uei,
        "award_amount":          record.award_amount,
        "agency":                record.agency,
        "sub_agency":            "",
        "naics":                 record.naics_code,
        "naics_description":     record.naics_description,
        "set_aside":             record.set_aside_code,
        "extent_competed":       record.extent_competed_code,
        "sole_source":           sole_source,
        "is_idiq":               False,  # already filtered out by reader
        "parent_recipient_name": record.parent_name,
    }


def stage1_load_and_filter(year: int = 2023, month_filter: int = 0) -> tuple[list[dict], dict[str, ContractRecord]]:
    """Load SAM.gov bulk CSV, filter by amount, remove top-N companies, write filtered CSV.

    Returns:
        (awards, records_by_key) where records_by_key maps piid → ContractRecord
        for use by Stage 2's V4 resolver.
    """
    log.info("=" * 60)
    log.info("STAGE 1: LOAD & FILTER")
    log.info(f"  Range  : ${MIN_CONTRACT_VALUE/1e6:.0f}M – ${MAX_AWARD_AMOUNT/1e9:.0f}B")
    log.info(f"  Remove : top {TOP_N_TO_REMOVE} companies by contract count")
    log.info(f"  Remove : IDV/IDIQ contracts (filtered at read time)")
    if month_filter:
        log.info(f"  Month  : {month_filter} (March=3)")
    log.info(f"  Output : {os.path.basename(FILTERED_CSV)}")
    log.info("=" * 60)
    t0 = time.time()

    sam_csv = find_sam_gov_csv(DATASET_DIR)
    if not sam_csv:
        log.error(f"No SAM.gov CSV found in {DATASET_DIR}/")
        log.error("Download a report from https://sam.gov/ and place it in datasets/")
        sys.exit(1)
    log.info(f"  Source: {os.path.basename(sam_csv)}")

    awards_by_key: dict[str, dict] = {}
    records_by_key: dict[str, ContractRecord] = {}
    total_rows = 0

    for record in read_sam_gov_csv(sam_csv):
        total_rows += 1

        if month_filter and record.posted_date:
            try:
                if int(record.posted_date.split("-")[1]) != month_filter:
                    continue
            except (ValueError, IndexError):
                pass

        awards_by_key[record.piid] = _record_to_award_dict(record)
        records_by_key[record.piid] = record

        if total_rows % 50_000 == 0:
            log.info(f"  ... {total_rows:,} rows read, {len(awards_by_key):,} unique awards so far")

    awards = list(awards_by_key.values())
    after_load = len(awards)
    log.info(f"  {total_rows:,} rows read → {after_load:,} awards (amount/IDV filtered at read time)")

    # ── Remove top-N companies by contract count ──────────────────────────────
    name_counts = Counter(a["awardee_name"] for a in awards)
    top_names = {name for name, _ in name_counts.most_common(TOP_N_TO_REMOVE)}
    log.info(f"  Top {TOP_N_TO_REMOVE} companies removed (by contract volume):")
    for name, cnt in name_counts.most_common(TOP_N_TO_REMOVE):
        log.info(f"    {cnt:>6,}  {name}")

    awards = [a for a in awards if a["awardee_name"] not in top_names]
    records_by_key = {a["award_key"]: records_by_key[a["award_key"]] for a in awards}
    dropped_top = after_load - len(awards)
    log.info(f"  Dropped {dropped_top:,} contracts from top-{TOP_N_TO_REMOVE} companies")
    log.info(f"  Final: {len(awards):,} contracts")

    _write_csv(FILTERED_CSV, awards)

    _save_cp(CP_STAGE1, {
        "total_rows_read": total_rows,
        "after_load": after_load,
        "dropped_top_n": dropped_top,
        "final_count": len(awards),
    })

    log.info(f"Stage 1 complete in {_elapsed(t0)}")
    return awards, records_by_key


def build_agency_history(awards: list[dict]) -> dict:
    """Count prior agency wins per award in chronological order.

    Returns dict: award_key → int (0 = first win from that agency).
    """
    sorted_awards = sorted(awards, key=lambda a: a.get("posted_date", ""))
    win_counts: dict[str, dict[str, int]] = {}  # company → {agency → count}
    history: dict[str, int] = {}
    for a in sorted_awards:
        name   = a["awardee_name"]
        agency = a.get("agency", "")
        key    = a["award_key"]
        if name not in win_counts:
            win_counts[name] = {}
        history[key] = win_counts[name].get(agency, 0)
        win_counts[name][agency] = win_counts[name].get(agency, 0) + 1
    return history


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — TICKER RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def _load_edgar_map() -> dict:
    """Load EDGAR company→ticker map (cached locally, refreshed weekly)."""
    if os.path.exists(EDGAR_MAP_FILE):
        age_days = (time.time() - os.path.getmtime(EDGAR_MAP_FILE)) / 86400
        if age_days < 7:
            log.info(f"  EDGAR map: loading from cache ({age_days:.1f}d old)...")
            with open(EDGAR_MAP_FILE) as f:
                data = json.load(f)
            # Migrate legacy format (bare ticker strings → dicts)
            migrated = 0
            for k, v in data.items():
                if isinstance(v, str):
                    data[k] = {"ticker": v, "cik": ""}
                    migrated += 1
            if migrated:
                with open(EDGAR_MAP_FILE, "w") as f:
                    json.dump(data, f)
            log.info(f"  EDGAR map: {len(data):,} companies")
            return data

    log.info("  Downloading EDGAR company tickers from SEC (cached 7 days)...")
    for attempt in range(4):
        try:
            resp = requests.get(EDGAR_TICKERS_URL, headers=EDGAR_HEADERS, timeout=30)
            resp.raise_for_status()
            raw = resp.json()
            break
        except Exception as e:
            log.warning(f"  EDGAR download attempt {attempt+1} failed: {e}")
            time.sleep(10 * (attempt + 1))
    else:
        log.error("  Could not download EDGAR ticker list")
        return {}

    edgar_map = {}
    for entry in raw.values():
        name   = entry.get("title", "").strip().upper()
        ticker = entry.get("ticker", "").strip().upper()
        cik    = str(entry.get("cik_str", ""))
        if name and ticker:
            edgar_map[name] = {"ticker": ticker, "cik": cik}

    with open(EDGAR_MAP_FILE, "w") as f:
        json.dump(edgar_map, f)
    log.info(f"  EDGAR map: {len(edgar_map):,} companies downloaded")
    return edgar_map


def stage2_resolve_tickers(
    awards: list[dict],
    records_by_key: dict[str, ContractRecord] | None = None,
) -> list[dict]:
    """Resolve each award's company to a ticker using TickerResolverV4.

    Deduplicates by V4 cache key (CAGE code → UEI → legal name → contractor name),
    resolves once per unique entity, then maps to all matching awards.
    """
    from ticker_resolver_v4 import TickerResolverV4

    log.info("=" * 60)
    log.info("STAGE 2: TICKER RESOLUTION (V4)")
    log.info(f"  Input  : {len(awards):,} filtered awards")
    log.info(f"  Cache  : {os.path.basename(TICKER_CACHE_V4_FILE)}")
    log.info(f"  Output : {os.path.basename(STAGE2_CSV)}")
    log.info("=" * 60)
    t0 = time.time()

    cp = _load_cp(CP_STAGE2)
    already_done = len(cp)
    if already_done:
        log.info(f"  Resuming: {already_done:,} awards already in checkpoint")

    edgar_map = _load_edgar_map()
    resolver  = TickerResolverV4(edgar_map, cache_path=TICKER_CACHE_V4_FILE)

    # Deduplicate by V4 cache key: resolve once per unique entity
    def _v4_key(award: dict) -> str:
        return (
            award.get("cage_code")
            or award.get("uei")
            or award.get("legal_business_name")
            or award.get("awardee_name")
            or ""
        )

    entity_key_to_award_keys: dict[str, list[str]] = {}
    skipped_count = 0
    for award in awards:
        award_key = award["award_key"]
        if award_key in cp:
            skipped_count += 1
        else:
            ek = _v4_key(award)
            entity_key_to_award_keys.setdefault(ek, []).append(award_key)

    # Pick one representative award_key per entity (to get its ContractRecord)
    unique_entity_keys = list(entity_key_to_award_keys.keys())
    # Build entity_key → ContractRecord mapping using first award in each group
    ek_to_record: dict[str, ContractRecord] = {}
    if records_by_key:
        for ek, award_keys in entity_key_to_award_keys.items():
            for ak in award_keys:
                if ak in records_by_key:
                    ek_to_record[ek] = records_by_key[ak]
                    break

    log.info(f"  {len(awards):,} awards → {len(unique_entity_keys):,} unique entities to resolve "
             f"({skipped_count:,} from checkpoint)")

    resolved_count = unresolved_count = 0
    CHECKPOINT_BATCH = 200

    for i, ek in enumerate(unique_entity_keys):
        record = ek_to_record.get(ek)
        if record is None:
            unresolved_count += 1
            entry = {"ticker": "", "cik": "", "ticker_confidence": "none"}
        else:
            result = resolver.resolve(record)
            ticker = result.get("resolved_ticker") or ""
            entry  = {
                "ticker":            ticker,
                "cik":               result.get("resolved_cik") or "",
                "ticker_confidence": result.get("confidence", "none"),
                "evidence_type":     result.get("evidence_type", ""),
            }
            if ticker:
                resolved_count += 1
            else:
                unresolved_count += 1

        for ak in entity_key_to_award_keys[ek]:
            cp[ak] = entry

        if (i + 1) % CHECKPOINT_BATCH == 0:
            _save_cp(CP_STAGE2, cp)
            pct = (i + 1) / len(unique_entity_keys) * 100
            pct_resolved = resolved_count / (i + 1) * 100
            log.info(f"  [{i+1:,}/{len(unique_entity_keys):,} — {pct:.1f}%] "
                     f"resolved={resolved_count:,} ({pct_resolved:.1f}%)  unresolved={unresolved_count:,}")
            print(f"[STAGE2_PROGRESS] {pct:.0f}% | Resolved: {resolved_count:,} | Unresolved: {unresolved_count:,}")

    resolver.save_cache()
    _save_cp(CP_STAGE2, cp)
    pct_resolved = resolved_count / len(unique_entity_keys) * 100 if unique_entity_keys else 0
    log.info(f"  Resolution complete: {resolved_count:,} resolved ({pct_resolved:.1f}%), "
             f"{unresolved_count:,} unresolved, {skipped_count:,} from checkpoint")
    print(f"[STAGE2_COMPLETE] {resolved_count:,} resolved | {unresolved_count:,} unresolved | {pct_resolved:.1f}%")

    enriched = []
    for award in awards:
        entry = cp.get(award["award_key"], {})
        enriched.append({**award, **entry})

    _write_csv(STAGE2_CSV, enriched)
    log.info(f"Stage 2 complete in {_elapsed(t0)}")
    return enriched


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3 — ENRICH (prices + shares + historical mcap + 8-K + dilutive + PR)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_shares(ticker: str) -> int:
    try:
        info = yf.Ticker(ticker).info
        return int(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding") or 0)
    except Exception:
        return 0


def _get_quarterly_balance_sheet(ticker: str):
    """Fetch quarterly balance sheet for a ticker. Returns DataFrame or None."""
    try:
        bs = yf.Ticker(ticker).quarterly_balance_sheet
        if bs is not None and not bs.empty:
            return bs
    except Exception:
        pass
    return None


def _get_historical_shares(ticker: str, date_str: str,
                            balance_sheet_cache: dict | None = None,
                            current_shares: int = 0,
                            splits_cache: dict | None = None) -> tuple[int, str]:
    """Get shares outstanding at a historical date.

    Tries quarterly balance sheet first (most accurate), falls back to
    split-adjusted current shares.

    Returns (shares: int, source: str) where source is "quarterly" or "split_adjusted".
    """
    import pandas as pd

    # ── Try quarterly balance sheet ──────────────────────────────────────────
    if balance_sheet_cache is not None and ticker:
        bs = balance_sheet_cache.get(ticker)
        if bs is not None and not bs.empty:
            try:
                target = pd.Timestamp(date_str)
                # Columns are quarter-end dates; find closest that's not >6 months after
                cols = pd.to_datetime(bs.columns)
                # Prefer the most recent quarter-end that is <= target + 6 months
                candidates = cols[cols <= target + pd.DateOffset(months=6)]
                if len(candidates) > 0:
                    # Pick the closest date
                    closest = candidates[candidates.get_indexer([target], method="nearest")[0]]
                    # Look for shares row (yfinance uses different names across versions)
                    for row_name in ("Ordinary Shares Number", "Share Issued",
                                     "Common Stock Shares Outstanding"):
                        if row_name in bs.index:
                            val = bs.loc[row_name, closest]
                            if pd.notna(val) and int(val) > 0:
                                return int(val), "quarterly"
            except Exception:
                pass

    # ── Fallback: split-adjusted current shares ──────────────────────────────
    if not current_shares or not date_str:
        return current_shares, "split_adjusted"
    try:
        if splits_cache is not None and ticker in splits_cache:
            splits = splits_cache[ticker]
        else:
            splits = yf.Ticker(ticker).splits
            if splits_cache is not None:
                splits_cache[ticker] = splits
        if splits.empty:
            return current_shares, "split_adjusted"
        target_dt = datetime.strptime(date_str, "%Y-%m-%d")
        if hasattr(splits.index, "tz") and splits.index.tz is not None:
            splits.index = splits.index.tz_localize(None)
        future_splits = splits[splits.index > target_dt]
        if future_splits.empty:
            return current_shares, "split_adjusted"
        adjustment = 1.0
        for ratio in future_splits:
            adjustment *= float(ratio)
        adjusted = int(current_shares / adjustment) if adjustment > 0 else current_shares
        return adjusted, "split_adjusted"
    except Exception:
        return current_shares, "split_adjusted"


def _fetch_year_history(ticker: str, year: int):
    """Fetch full-year OHLC history for a ticker. Returns DataFrame or None."""
    import pandas as pd
    try:
        hist = yf.Ticker(ticker).history(
            start=f"{year}-01-01",
            end=f"{year + 1}-01-15",
            auto_adjust=True,
            timeout=30,
        )
        if hist.empty:
            return None
        if hasattr(hist.index, "tz") and hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        return hist
    except Exception as e:
        log.warning(f"  {ticker} {year}: history fetch failed — {e}")
        return None


def _slice_price_window(hist, date_str: str, n_days: int = 7) -> dict:
    """Slice n_days of OHLC from a pre-fetched full-year DataFrame.

    Returns open/high/low/close/price/return columns for t0..t{n}.
    """
    if hist is None or hist.empty or not date_str:
        return {}
    try:
        import pandas as pd
        start_dt = datetime.strptime(date_str, "%Y-%m-%d")
        future = hist[hist.index >= pd.Timestamp(start_dt)]
        if future.empty:
            return {}
        sliced = future.iloc[: n_days + 1]
        prices: dict = {}
        for i in range(len(sliced)):
            row = sliced.iloc[i]
            o = round(float(row.get("Open",  0)), 4)
            h = round(float(row.get("High",  0)), 4)
            l = round(float(row.get("Low",   0)), 4)
            c = round(float(row.get("Close", 0)), 4)
            prices[f"open_t{i}"]  = o
            prices[f"high_t{i}"]  = h
            prices[f"low_t{i}"]   = l
            prices[f"close_t{i}"] = c
            prices[f"price_t{i}"] = c  # alias used elsewhere
        if prices.get("price_t0", 0) > 0:
            t0p = prices["price_t0"]
            for i in range(len(sliced)):
                p = prices[f"price_t{i}"]
                if p > 0:
                    prices[f"return_t{i}"] = round((p / t0p - 1) * 100, 4)
                else:
                    prices[f"return_t{i}"] = ""
        return prices
    except Exception as e:
        log.debug(f"Price slice error {date_str}: {e}")
        return {}


def _fetch_edgar_submissions(cik: str) -> dict:
    """Fetch EDGAR submissions JSON for a CIK. Returns {} on failure."""
    if not cik or not str(cik).strip().isdigit():
        return {}
    _edgar_throttle()
    try:
        url  = EDGAR_SUBMISSIONS_URL.format(cik=str(cik).zfill(10))
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=25)
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"  EDGAR fetch error CIK={cik}: {e}")
        return {}


def _first_8k_info(submissions: dict, contract_date_str: str) -> tuple[str, str]:
    """Find the first 8-K within EDGAR_8K_WINDOW_DAYS after the contract date.

    Returns (first_8k_date, hours_to_8k) — both empty strings if none found.
    hours_to_8k is day-granularity (EDGAR only stores dates): days × 24.
    """
    if not submissions or not contract_date_str:
        return "", ""
    try:
        recent = submissions.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])
        contract_dt = datetime.strptime(contract_date_str, "%Y-%m-%d")
        lo = contract_dt - timedelta(days=1)
        hi = contract_dt + timedelta(days=EDGAR_8K_WINDOW_DAYS)
        earliest: datetime | None = None
        earliest_str = ""
        for form, d in zip(forms, dates):
            if form != "8-K":
                continue
            try:
                fd = datetime.strptime(d, "%Y-%m-%d")
            except Exception:
                continue
            if lo <= fd <= hi:
                if earliest is None or fd < earliest:
                    earliest = fd
                    earliest_str = d
        if earliest is None:
            return "", ""
        hours = str((earliest - contract_dt).days * 24)
        return earliest_str, hours
    except Exception:
        return "", ""


def _find_last_dilutive_before_date(submissions: dict, contract_date_str: str,
                                    days_before: int = 180) -> tuple[str, str]:
    """Find the most recent S-1/S-3 filing within days_before of the contract date.

    Returns (filing_date, form_type) or ("", "").
    """
    if not submissions or not contract_date_str:
        return "", ""
    try:
        recent = submissions.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])
        dilutive = {"S-1", "S-3", "S-1/A", "S-3/A"}
        contract_dt = datetime.strptime(contract_date_str, "%Y-%m-%d")
        lo = contract_dt - timedelta(days=days_before)
        latest_date = ""
        latest_form = ""
        for form, d in zip(forms, dates):
            if form not in dilutive:
                continue
            try:
                fd = datetime.strptime(d, "%Y-%m-%d")
            except Exception:
                continue
            if lo <= fd <= contract_dt:
                if d > latest_date:
                    latest_date = d
                    latest_form = form
        return latest_date, latest_form
    except Exception:
        return "", ""


def stage3_enrich(awards: list[dict], agency_history: dict) -> list[dict]:
    """Fetch OHLC prices, shares, historical mcap, 8-K, dilutive filings for ticker-resolved awards.

    Optimizations:
      - Batch price fetching: one yfinance history call per (ticker, year), sliced locally
      - Shares and splits cached per ticker (not per award)
      - EDGAR submissions cached per CIK
      - Checkpoint stores enrichment delta only (not full award row)
    """
    qualifying = [a for a in awards if a.get("ticker")]
    log.info("=" * 60)
    log.info("STAGE 3: ENRICH")
    log.info(f"  Input    : {len(awards):,} total, {len(qualifying):,} have a ticker")
    log.info(f"  Fetching : OHLC t0-t7, shares, historical mcap, 8-K, dilutive filings")
    log.info(f"  Output   : {os.path.basename(FINAL_CSV)}")
    log.info("=" * 60)
    t0 = time.time()

    cp = _load_cp(CP_STAGE3)
    if cp:
        # Invalidate old-format checkpoints that lack quarterly balance sheet data
        sample = next(iter(cp.values()), {})
        if "shares_source" not in sample:
            log.warning("  Stage 3 checkpoint uses old shares method — clearing to recompute "
                        "with quarterly balance sheet data")
            cp = {}
        else:
            log.info(f"  Resuming: {len(cp):,} awards already in checkpoint")

    # ── Pre-fetch: batch OHLC histories per (ticker, year) ────────────────────
    # Figure out which (ticker, year) combos we actually need (skip checkpointed awards)
    needed = [a for a in qualifying if a["award_key"] not in cp]
    ticker_years: dict[tuple[str, int], list] = {}
    for a in needed:
        ticker = a["ticker"]
        date_str = a.get("posted_date", "")
        if ticker and date_str:
            try:
                year = int(date_str[:4])
            except (ValueError, IndexError):
                continue
            ticker_years.setdefault((ticker, year), []).append(a)

    # Fetch full-year histories in parallel (one call per unique ticker-year)
    history_cache: dict[tuple[str, int], object] = {}  # (ticker, year) → DataFrame|None
    if ticker_years:
        log.info(f"  Pre-fetching {len(ticker_years):,} unique (ticker, year) histories "
                 f"for {len(needed):,} awards...")
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(_fetch_year_history, tk, yr): (tk, yr)
                for tk, yr in ticker_years
            }
            done = 0
            for fut in as_completed(futures):
                tk_yr = futures[fut]
                history_cache[tk_yr] = fut.result()
                done += 1
                if done % 50 == 0:
                    log.info(f"    {done:,}/{len(ticker_years):,} histories fetched")
        fetched_ok = sum(1 for v in history_cache.values() if v is not None)
        log.info(f"  Histories fetched: {fetched_ok:,} OK, "
                 f"{len(history_cache) - fetched_ok:,} failed")

    # ── Pre-fetch: shares + splits + quarterly balance sheets per unique ticker
    unique_tickers_needed = {a["ticker"] for a in needed if a.get("ticker")}
    shares_cache: dict[str, int] = {}
    splits_cache: dict[str, object] = {}
    balance_sheet_cache: dict[str, object] = {}
    if unique_tickers_needed:
        log.info(f"  Pre-fetching shares + balance sheets for {len(unique_tickers_needed):,} unique tickers...")
        with ThreadPoolExecutor(max_workers=8) as pool:
            share_futs = {pool.submit(_get_shares, t): t for t in unique_tickers_needed}
            bs_futs = {pool.submit(_get_quarterly_balance_sheet, t): t for t in unique_tickers_needed}
            for fut in as_completed(share_futs):
                t = share_futs[fut]
                shares_cache[t] = fut.result()
            for fut in as_completed(bs_futs):
                t = bs_futs[fut]
                balance_sheet_cache[t] = fut.result()
        bs_ok = sum(1 for v in balance_sheet_cache.values() if v is not None)
        log.info(f"  Balance sheets: {bs_ok:,}/{len(unique_tickers_needed):,} available")

    # ── Per-award enrichment (EDGAR is rate-limited, must be sequential) ──────
    submissions_cache: dict[str, dict] = {}   # cik → EDGAR submissions
    enriched_count = skipped = 0
    CHECKPOINT_BATCH = 50

    for i, award in enumerate(qualifying):
        key = award["award_key"]

        if key in cp:
            skipped += 1
            continue

        ticker   = award.get("ticker", "")
        cik      = award.get("cik", "")
        date_str = award.get("posted_date", "")

        log.debug(f"  [{i+1}/{len(qualifying)}] {award['awardee_name']} | {ticker} | "
                  f"{date_str} | ${float(award.get('award_amount', 0)):,.0f}")

        # ── OHLC prices: slice from pre-fetched history ───────────────────
        year_key = None
        if ticker and date_str:
            try:
                year_key = (ticker, int(date_str[:4]))
            except (ValueError, IndexError):
                pass
        hist_df = history_cache.get(year_key) if year_key else None
        prices = _slice_price_window(hist_df, date_str)

        if prices:
            log.debug(f"    open_t0={prices.get('open_t0','?')}  close_t7={prices.get('close_t7','?')}")
        else:
            log.warning(f"    no price data for {ticker} on {date_str}")

        # ── Historical market cap (quarterly balance sheet → split-adjusted fallback)
        if ticker and date_str:
            hist_shares, shares_source = _get_historical_shares(
                ticker, date_str,
                balance_sheet_cache=balance_sheet_cache,
                current_shares=shares_cache.get(ticker, 0),
                splits_cache=splits_cache,
            )
        else:
            hist_shares, shares_source = shares_cache.get(ticker, 0), "split_adjusted"
        t0_price = prices.get("price_t0", 0) or 0
        hist_mcap = int(t0_price * hist_shares) if t0_price and hist_shares else 0
        if hist_mcap:
            log.debug(f"    hist_mcap ~${hist_mcap/1e6:.1f}M  (price={t0_price}  shares={hist_shares:,}  src={shares_source})")

        # ── EDGAR submissions (rate-limited, cached per CIK) ──────────────
        if cik and cik not in submissions_cache:
            log.debug(f"    fetching EDGAR for CIK {cik}...")
            submissions_cache[cik] = _fetch_edgar_submissions(cik)
        subs = submissions_cache.get(cik, {})

        first_8k_date, hours_to_8k = _first_8k_info(subs, date_str)
        dilutive_date, dilutive_type = _find_last_dilutive_before_date(subs, date_str)
        log.debug(f"    first_8k={first_8k_date or 'none'}  dilutive={dilutive_date or 'none'}")

        # ── Agency history ────────────────────────────────────────────────
        prior_wins = agency_history.get(key, 0)

        # Store only the enrichment delta in checkpoint (not the full award)
        enrich_data = {
            # OHLC columns (open/high/low/close/price/return per day)
            **{f"open_t{j}":   prices.get(f"open_t{j}",   "") for j in range(8)},
            **{f"high_t{j}":   prices.get(f"high_t{j}",   "") for j in range(8)},
            **{f"low_t{j}":    prices.get(f"low_t{j}",    "") for j in range(8)},
            **{f"close_t{j}":  prices.get(f"close_t{j}",  "") for j in range(8)},
            **{f"price_t{j}":  prices.get(f"price_t{j}",  "") for j in range(8)},
            **{f"return_t{j}": prices.get(f"return_t{j}", "") for j in range(8)},
            # Market cap columns
            "historical_market_cap_approx": hist_mcap,
            "shares_outstanding_approx":    shares_cache.get(ticker, 0),
            "shares_outstanding_historical": hist_shares,
            "shares_source":                shares_source,
            # EDGAR
            "first_8k_date":              first_8k_date,
            "hours_to_8k":                hours_to_8k,
            "last_dilutive_filing_date":   dilutive_date,
            "dilutive_filing_type":        dilutive_type,
            # PR (not yet implemented — mark as unknown so scoring doesn't give free points)
            "first_pr_date":  "",
            "has_pr":         "unknown",
            # Agency history
            "agency_prior_win_count": prior_wins,
        }
        cp[key] = enrich_data
        enriched_count += 1
        if enriched_count % CHECKPOINT_BATCH == 0:
            _save_cp(CP_STAGE3, cp)

        if (i + 1) % 500 == 0:
            pct = (i + 1) / len(qualifying) * 100
            log.info(f"  PROGRESS: {pct:.1f}% ({i+1}/{len(qualifying)}) "
                     f"enriched={enriched_count:,}  skipped={skipped:,}")

    _save_cp(CP_STAGE3, cp)  # final flush
    log.info(f"  Enriched {enriched_count:,} new, {skipped:,} from checkpoint")

    # Assemble final rows: merge award + enrichment delta from checkpoint
    qualifying_keys = {a["award_key"] for a in qualifying}
    empty_enrich = {
        **{f"open_t{j}": "" for j in range(8)},
        **{f"high_t{j}": "" for j in range(8)},
        **{f"low_t{j}": "" for j in range(8)},
        **{f"close_t{j}": "" for j in range(8)},
        **{f"price_t{j}": "" for j in range(8)},
        **{f"return_t{j}": "" for j in range(8)},
        "historical_market_cap_approx": "",
        "shares_outstanding_approx": "",
        "shares_outstanding_historical": "",
        "shares_source": "",
        "first_8k_date": "",
        "hours_to_8k": "",
        "last_dilutive_filing_date": "",
        "dilutive_filing_type": "",
        "first_pr_date": "",
        "has_pr": "unknown",
        "agency_prior_win_count": "",
    }
    final_rows: list[dict] = []
    for award in awards:
        key = award["award_key"]
        if key in qualifying_keys and key in cp:
            final_rows.append({**award, **cp[key]})
        else:
            final_rows.append({**award, **empty_enrich,
                                "agency_prior_win_count": agency_history.get(key, "")})

    _write_csv(FINAL_CSV, final_rows)
    log.info(f"Stage 3 complete in {_elapsed(t0)}")
    return final_rows


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Build training dataset for SAMgovArby")
    add_verbosity_flags(parser)
    args = parser.parse_args()

    # Initialize logger with user's verbosity preference
    global log
    log = setup_logging("build", quiet=args.quiet, verbose=args.verbose, json_format=args.json)

    os.makedirs(DATASET_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    run_start = time.time()
    year = 2023
    month_filter = 0  # All months (full year)

    # Stage 1 — load & filter (fast, reads local file)
    awards, records_by_key = stage1_load_and_filter(year, month_filter=month_filter)

    # Agency history — cheap in-memory pass over filtered set
    agency_history = build_agency_history(awards)
    first_wins = sum(1 for v in agency_history.values() if v == 0)
    log.info(f"Agency history: {first_wins:,} first-time agency wins out of {len(agency_history):,}")

    # Stage 2 — ticker resolution (sequential, resumable)
    awards = stage2_resolve_tickers(awards, records_by_key=records_by_key)

    # Stage 3 — enrich (prices, shares, historical mcap, 8-K, dilutive filings)
    final = stage3_enrich(awards, agency_history)

    total = _elapsed(run_start)
    enriched = sum(1 for r in final if r.get("hours_to_8k") != "" or r.get("price_t0") != "")
    with_ticker = sum(1 for r in final if r.get("ticker"))

    log.info("")
    log.info("=" * 60)
    log.info("  BUILD COMPLETE")
    log.info("=" * 60)
    log.info(f"  Runtime          : {total}")
    log.info(f"  Total rows       : {len(final):,}")
    log.info(f"  With ticker      : {with_ticker:,}")
    log.info(f"  Fully enriched   : {enriched:,}")
    log.info(f"  Output           : {FINAL_CSV}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
