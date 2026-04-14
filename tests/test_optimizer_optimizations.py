"""
Correctness tests for optimizer performance optimizations.

Tests verify three properties:
  1. _stats() produces the same numbers as the original statistics-module reference.
  2. Date integer pre-computation gives identical overlap-check results as the
     original string-based comparison.
  3. _sweep_dz_worker produces identical opt_rows to a reference implementation
     of the original sequential Phase 2 inner loop on the same synthetic data.
"""
import statistics as _stdlib_stats
from datetime import date as _date, timedelta

import pytest


# ── helpers ──────────────────────────────────────────────────────────────────

def _ref_stats(pnls: list) -> dict:
    """Reference implementation using the stdlib statistics module."""
    n = len(pnls)
    if n == 0:
        return {}
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    wr     = len(wins) / n
    avg_w  = sum(wins)   / len(wins)   if wins   else 0.0
    avg_l  = abs(sum(losses) / len(losses)) if losses else 0.0
    exp    = (wr * avg_w) - ((1 - wr) * avg_l)
    std    = _stdlib_stats.stdev(pnls) if n >= 2 else 0.0
    return {
        "std_dev_pnl": round(std, 4),
        "expectancy":  round(exp, 4),
        "win_rate":    round(wr * 100, 1),
        "avg_pnl_pct": round(sum(pnls) / n, 3),
    }


def _reference_sweep_dz(dz_config: tuple, state: dict) -> tuple:
    """Exact mirror of the ORIGINAL Phase 2 inner loop (pre-optimisation).

    Used to verify that _sweep_dz_worker produces identical results.
    """
    from datetime import date as _d
    from optimizer import _stats, _rank_score, MCAP_STEP, MCAP_MAX, normalize_date

    dz_min, dz_max        = dz_config
    excluded              = state['dz_excluded'][(dz_min, dz_max)]
    sim_cache             = state['sim_cache']
    row_score_cache_full  = state['_row_score_cache_full']   # full list incl. None entries
    base_combos           = state['base_combos']
    date_range_days       = state['date_range_days']

    best_score = -999.0
    best_combo = None
    opt_rows: list = []

    for threshold, tp, sl, hold in base_combos:
        sim_results = sim_cache[(tp, sl, hold)]

        eligible: list = []
        seen_trades: set = set()
        ticker_last_entry_opt: dict = {}

        for row_idx, cached in enumerate(row_score_cache_full):
            if cached is None:
                continue
            if row_idx in excluded:
                continue
            raw_score, extra, row = cached
            if raw_score < threshold:
                continue
            ticker   = row.get("ticker", "")
            date_str = normalize_date(row.get("posted_date", ""))[:10]
            key = (ticker, date_str)
            if key in seen_trades:
                continue
            if ticker and ticker in ticker_last_entry_opt:
                try:
                    delta = (_d.fromisoformat(date_str)
                             - _d.fromisoformat(ticker_last_entry_opt[ticker])).days
                    if 0 < delta <= hold:
                        continue
                except (ValueError, TypeError):
                    pass
            seen_trades.add(key)
            if ticker:
                ticker_last_entry_opt[ticker] = date_str
            sim_result = sim_results[row_idx]
            if sim_result:
                eligible.append((extra.get("market_cap", 0), sim_result[0], sim_result[1]))

        if not eligible:
            opt_rows.append(_stats([], tp, sl, threshold, hold, MCAP_MAX, date_range_days, dz_min, dz_max))
            continue

        eligible.sort()
        ei, n_el        = 0, len(eligible)
        pnls: list      = []
        peaks: list     = []
        last_emitted_ei = -1

        buckets: set = set()
        for mcap, _, _ in eligible:
            buckets.add(((int(mcap) // MCAP_STEP) + 1) * MCAP_STEP)
        buckets.add(MCAP_MAX)
        mcap_thresholds = sorted(b for b in buckets if b <= MCAP_MAX)

        for max_mcap in mcap_thresholds:
            while ei < n_el and eligible[ei][0] <= max_mcap:
                pnls.append(eligible[ei][1])
                peaks.append(eligible[ei][2])
                ei += 1
            if not pnls or ei == last_emitted_ei:
                continue
            last_emitted_ei = ei
            trades = [{"pnl": p, "peak": pk} for p, pk in zip(pnls, peaks)]
            stats = _stats(trades, tp, sl, threshold, hold, max_mcap, date_range_days, dz_min, dz_max)
            opt_rows.append(stats)
            combo_score = _rank_score(stats)
            if combo_score > best_score:
                best_score = combo_score
                best_combo = stats

    return opt_rows, best_score, best_combo


def _make_synthetic_state(n_valid: int = 20) -> dict:
    """Build a minimal _WORKER_STATE-compatible dict for direct worker testing.

    Includes both valid_row_entries (for the worker) and _row_score_cache_full
    (for the reference implementation).
    """
    EPOCH   = _date(1970, 1, 1)
    base_dt = _date(2023, 1, 2)   # Monday
    mcap    = 50_000_000           # $50 M

    row_score_cache_full: list = []
    row_dates_int: list        = []

    # Interleave None entries (filtered rows) with valid entries to mimic real data.
    for i in range(n_valid * 2):
        if i % 2 == 0:                          # even positions → filtered out
            row_score_cache_full.append(None)
            row_dates_int.append(None)
            continue

        idx = i // 2
        dt  = base_dt + timedelta(days=idx * 7) # one per week → no overlap conflicts
        row = {
            "ticker":       f"SYN{idx:02d}",
            "posted_date":  dt.isoformat(),
            "award_amount": "5000000",
            "agency":       "ARMY",
            "awardee_name": f"Corp {idx}",
            "naics":        "541330",
            "sole_source":  "false",
        }
        extra = {
            "market_cap":            mcap,
            "has_press_release":     False,
            "agency_prior_win_count": 0,
            "value_to_mcap":         0.10,   # outside all dead zones tested
        }
        score = 50.0 if idx % 2 == 0 else 20.0  # alternating high/low scores

        row_score_cache_full.append((score, extra, row))
        row_dates_int.append((_date.fromisoformat(dt.isoformat()) - EPOCH).days)

    valid_row_entries = [
        (i, c) for i, c in enumerate(row_score_cache_full) if c is not None
    ]

    # Tiny (tp, sl, hold) grid so tests run quickly.
    tp_values   = [0.05, 0.10]
    sl_values   = [0.03, 0.05]
    hold_values = [2, 3]

    sim_cache: dict = {}
    for tp in tp_values:
        for sl in sl_values:
            for hold in hold_values:
                results = []
                for i, cached in enumerate(row_score_cache_full):
                    if cached is None:
                        results.append(None)
                        continue
                    idx = i // 2
                    # Even idx = winner (hits TP), odd = loser (hits SL)
                    if idx % 2 == 0:
                        results.append((round(tp * 100 * 0.9, 4), round(tp * 100 * 1.1, 4)))
                    else:
                        results.append((round(-sl * 100 * 0.8, 4), round(sl * 100 * 0.2, 4)))
                sim_cache[(tp, sl, hold)] = results

    base_combos = [
        (threshold, tp, sl, hold)
        for threshold in [10, 30, 60]
        for tp in tp_values
        for sl in sl_values
        for hold in hold_values
    ]

    # Dead zone that excludes rows with v2m == 0.10 (all valid rows in this dataset)
    dz_excluded = {
        (0.0, 0.0):   set(),
        (0.04, 0.12):  {i for i, c in enumerate(row_score_cache_full)
                        if c is not None and c[1].get("value_to_mcap", 0) <= 0.12},
    }

    return {
        'sim_cache':             sim_cache,
        'valid_row_entries':     valid_row_entries,
        'row_dates_int':         row_dates_int,
        'base_combos':           base_combos,
        'dz_excluded':           dz_excluded,
        'date_range_days':       n_valid * 7,
        '_row_score_cache_full': row_score_cache_full,  # reference only
    }


# ── 1. _stats() unit tests ───────────────────────────────────────────────────

def test_stats_matches_stdlib_reference():
    """numpy std should give same rounded result as statistics.stdev."""
    from optimizer import _stats

    pnls   = [0.05, -0.03, 0.08, -0.02, 0.04, 0.06, -0.01, 0.03, -0.04, 0.07]
    trades = [{"pnl": p, "peak": abs(p)} for p in pnls]
    result = _stats(trades, tp=0.08, sl=0.03, threshold=20, hold=3,
                    max_market_cap=100_000_000)
    ref    = _ref_stats(pnls)

    assert result["trades"]      == 10
    assert result["std_dev_pnl"] == ref["std_dev_pnl"]
    assert result["expectancy"]  == ref["expectancy"]
    assert result["win_rate"]    == ref["win_rate"]
    assert result["avg_pnl_pct"] == ref["avg_pnl_pct"]


def test_stats_empty_trades():
    from optimizer import _stats
    r = _stats([], tp=0.05, sl=0.03, threshold=10, hold=2, max_market_cap=50_000_000)
    assert r["trades"]      == 0
    assert r["expectancy"]  == -999
    assert r["std_dev_pnl"] == 0


def test_stats_single_winner():
    from optimizer import _stats
    r = _stats([{"pnl": 0.10, "peak": 0.12}], tp=0.10, sl=0.03, threshold=10, hold=2)
    assert r["trades"]      == 1
    assert r["win_rate"]    == 100.0
    assert r["std_dev_pnl"] == 0       # n < 2 → no std dev


def test_stats_all_losers():
    from optimizer import _stats
    pnls   = [-0.03, -0.05, -0.02]
    trades = [{"pnl": p, "peak": 0.0} for p in pnls]
    r      = _stats(trades, tp=0.05, sl=0.03, threshold=10, hold=2)
    assert r["win_rate"]   == 0.0
    assert r["avg_win"]    == 0.0
    assert r["expectancy"] < 0


# ── 2. Date integer precomputation tests ─────────────────────────────────────

def test_date_int_overlap_matches_string_comparison():
    """Integer delta must reproduce the same overlap decision as _date.fromisoformat."""
    EPOCH = _date(1970, 1, 1)

    def to_int(s: str) -> int:
        return (_date.fromisoformat(s) - EPOCH).days

    def orig_overlap(d1: str, d2: str, hold: int) -> bool:
        return 0 < (_date.fromisoformat(d1) - _date.fromisoformat(d2)).days <= hold

    def new_overlap(d1: str, d2: str, hold: int) -> bool:
        return 0 < (to_int(d1) - to_int(d2)) <= hold

    cases = [
        ("2023-06-15", "2023-06-12", 3),   # delta 3 == hold → overlap
        ("2023-06-16", "2023-06-12", 3),   # delta 4 > hold  → no overlap
        ("2023-06-12", "2023-06-15", 3),   # negative delta  → no overlap
        ("2023-06-12", "2023-06-12", 3),   # delta 0         → no overlap
        ("2023-12-31", "2023-12-29", 1),   # cross year-end boundary
    ]
    for d1, d2, hold in cases:
        assert orig_overlap(d1, d2, hold) == new_overlap(d1, d2, hold), \
            f"Mismatch: {d1} vs {d2} hold={hold}"


# ── 3. Worker vs reference comparison ────────────────────────────────────────

@pytest.mark.parametrize("dz_config", [(0.0, 0.0), (0.04, 0.12)])
def test_sweep_worker_matches_reference(dz_config):
    """_sweep_dz_worker must produce identical opt_rows to the original loop."""
    import optimizer as opt

    state = _make_synthetic_state(n_valid=20)
    opt._mp_init(state)

    worker_rows, w_best, w_combo = opt._sweep_dz_worker(dz_config)
    ref_rows,    r_best, r_combo = _reference_sweep_dz(dz_config, state)

    assert len(worker_rows) == len(ref_rows), (
        f"Row count mismatch for dz={dz_config}: "
        f"worker={len(worker_rows)} ref={len(ref_rows)}"
    )

    def sort_key(r):
        return (
            r["score_threshold"], r["tp_pct"], r["sl_pct"],
            r["max_hold_days"], r.get("max_market_cap_m") or 0,
        )

    for w, r in zip(sorted(worker_rows, key=sort_key),
                    sorted(ref_rows,    key=sort_key)):
        assert w == r, f"Row mismatch for dz={dz_config}:\nWorker: {w}\nRef:    {r}"

    assert abs(w_best - r_best) < 1e-9, \
        f"best_score mismatch: worker={w_best} ref={r_best}"


def test_sweep_worker_deterministic():
    """Running the worker twice on the same state must yield identical output."""
    import optimizer as opt

    state = _make_synthetic_state(n_valid=20)
    opt._mp_init(state)

    rows1, s1, _ = opt._sweep_dz_worker((0.0, 0.0))
    rows2, s2, _ = opt._sweep_dz_worker((0.0, 0.0))

    assert rows1 == rows2
    assert s1    == s2
