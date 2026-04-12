# Optimizer Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove market cap limiting, implement SQN ranking, granular parameter grids, fix all audit bugs, and update the GUI optimizer panel to show all relevant metrics.

**Architecture:** Three files change: `optimizer.py` (core logic), `filter_engine_bt.py` (remove mcap upper-bound filter), `backtest.py` (remove mcap arg), and `gui.py` (new stat cards, removed mcap signal param). No new files needed.

**Tech Stack:** Python 3, PyQt6, statistics stdlib, existing price_sim/scoring_engine/filter_engine_bt modules.

---

## File Map

| File | Change |
|---|---|
| `optimizer.py` | Remove mcap from grid; granular threshold/tp/sl; SQN ranking; std_dev/trades_per_week/avg_pnl_per_week in stats; pre-compute scores; fix labels; remove dead import |
| `filter_engine_bt.py` | Remove `max_market_cap` param and Filter 4 (upper mcap bound) |
| `backtest.py` | Remove `--max-market-cap` arg and all `max_market_cap` plumbing |
| `gui.py` | Remove mcap signal param; new stat cards; fix `_apply_best` |

---

### Task 1: Remove market cap upper-bound from filter engine

**Files:**
- Modify: `filter_engine_bt.py`

- [ ] Remove `max_market_cap` parameter from `apply_filters_bt_from_training` signature and all internal logic. Keep Filter 3 (mcap > 0 data check). The function signature becomes:

```python
def apply_filters_bt_from_training(row):
```

Remove these lines entirely:
```python
effective_mcap_limit = max_market_cap if max_market_cap is not None else MAX_MARKET_CAP
```
and:
```python
if hist_mcap > effective_mcap_limit:
    return False, f"Historical market cap ${hist_mcap/1e6:.0f}M exceeds ${effective_mcap_limit/1e6:.0f}M limit", extra
```

Also remove `MAX_MARKET_CAP` from the import at the top since it's no longer used here:
```python
from config import (MIN_CONTRACT_VALUE,
                    MAX_8K_WINDOW_DAYS, MAX_DILUTIVE_WINDOW_DAYS,
                    MAX_PR_WINDOW_DAYS, MIN_TICKER_CONFIDENCE)
```

- [ ] Verify the file by reading it back — confirm `max_market_cap` does not appear anywhere.

---

### Task 2: Strip market cap from backtest.py

**Files:**
- Modify: `backtest.py`

- [ ] Remove `max_market_cap` from `run_backtest` signature (line ~147), its log string, and the pass-through to `_simulate_batch`.

- [ ] Remove `max_market_cap` from `_simulate_batch` signature (line ~170) and its pass-through to `_process_training_row`.

- [ ] Remove `max_market_cap` from `_process_training_row` signature (line ~259) and update the call to `apply_filters_bt_from_training`:
```python
passed, reason, extra = apply_filters_bt_from_training(row)
```

- [ ] Remove the `--max-market-cap` argparse argument and `args.max_market_cap` usage at the bottom of the file.

- [ ] Verify: `grep max_market_cap backtest.py` should return zero matches.

---

### Task 3: Overhaul optimizer.py — grid, SQN, stats, fixes

**Files:**
- Modify: `optimizer.py`

- [ ] **Replace PARAM_GRID** with granular values and no market cap dimension:

```python
PARAM_GRID = {
    "score_threshold": list(range(1, 51)),                        # 1–50 inclusive
    "take_profit_pct": [i / 100 for i in range(1, 21)],          # 1%–20% in 1% steps
    "stop_loss_pct":   [i / 100 for i in range(1, 20)],          # 1%–19% in 1% steps
    "max_hold_days":   [1, 2, 3, 4, 5, 7],
}
```

- [ ] **Remove dead `import tempfile`** from `optimize_from_api`.

- [ ] **Fix `_rank_score`** to use SQN (Van Tharp, n capped at 100, min 15 trades):

```python
def _rank_score(stats) -> float:
    """SQN: (expectancy / std_dev) * sqrt(min(n, 100)).
    Requires >= 15 trades. Higher is better.
    """
    import math
    n = stats.get("trades", 0)
    if n < 15:
        return -999
    exp = stats.get("expectancy", 0)
    std = stats.get("std_dev_pnl", 0)
    if std <= 0:
        return -999
    return (exp / std) * math.sqrt(min(n, 100))
```

- [ ] **Update `_stats`** signature to accept `date_range_days` and compute new fields. Replace the entire `_stats` function:

```python
def _stats(trades, tp, sl, threshold, hold, date_range_days=None):
    """Calculate stats from trades list (dicts with pnl/peak keys)."""
    import statistics as _stats_mod
    n = len(trades)
    base = {"trades": 0, "win_rate": 0, "avg_pnl_pct": 0,
            "total_pnl_pct": 0, "expectancy": -999, "std_dev_pnl": 0,
            "profit_factor": 0, "avg_win": 0, "avg_loss": 0,
            "avg_peak_pnl": 0, "peak_pnl_pct": 0, "max_drawdown_pct": 0,
            "trades_per_week": 0, "avg_pnl_per_week": 0,
            "tp_pct": tp * 100, "sl_pct": sl * 100,
            "score_threshold": threshold, "max_hold_days": hold}
    if n == 0:
        return base

    pnls  = [t if isinstance(t, (int, float)) else t["pnl"] for t in trades]
    peaks = [t.get("peak", 0) if isinstance(t, dict) else 0 for t in trades]

    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    avg    = sum(pnls) / n
    win_rate = len(wins) / n

    avg_win  = sum(wins) / len(wins) if wins else 0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0
    avg_peak = sum(peaks) / n if peaks else 0

    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    std_dev    = _stats_mod.stdev(pnls) if n >= 2 else 0

    gross_wins   = sum(wins) if wins else 0
    gross_losses = abs(sum(losses)) if losses else 0
    if gross_losses > 0:
        profit_factor = min(gross_wins / gross_losses, 99.0)
    elif gross_wins > 0:
        profit_factor = 99.0
    else:
        profit_factor = 0.0

    # Max drawdown on cumulative P&L stream
    cumulative, peak_cum, max_dd = 0, 0, 0
    for p in pnls:
        cumulative += p
        if cumulative > peak_cum:
            peak_cum = cumulative
        dd = peak_cum - cumulative
        if dd > max_dd:
            max_dd = dd

    peak_pnl = max(pnls) if pnls else 0

    # Temporal metrics
    weeks = (date_range_days / 7) if date_range_days and date_range_days > 0 else None
    trades_per_week  = round(n / weeks, 2) if weeks else 0
    avg_pnl_per_week = round(sum(pnls) / weeks, 2) if weeks else 0

    base.update({
        "trades": n,
        "win_rate": round(win_rate * 100, 1),
        "avg_pnl_pct": round(avg, 3),
        "total_pnl_pct": round(sum(pnls), 2),
        "expectancy": round(expectancy, 4),
        "std_dev_pnl": round(std_dev, 4),
        "profit_factor": round(profit_factor, 3),
        "avg_win": round(avg_win, 3),
        "avg_loss": round(avg_loss, 3),
        "avg_peak_pnl": round(avg_peak, 2),
        "peak_pnl_pct": round(peak_pnl, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "trades_per_week": trades_per_week,
        "avg_pnl_per_week": avg_pnl_per_week,
    })
    return base
```

- [ ] **Update `optimize_from_training_csv`**:
  - Remove `max_market_cap` combo dimension and per-combo mcap filter block
  - Pre-compute score once per row (call `score_contract` with threshold=0, cache raw score)
  - Compute `date_range_days` from min/max posted_date of eligible rows
  - Pass `date_range_days` to `_stats`
  - Pass `date_range_days` to `_stats` calls

  Replace the combo loop section (after row_filter_cache is built) with:

```python
    # Pre-compute raw scores once per row (threshold comparison happens per combo)
    row_score_cache = []
    for row_idx, row in enumerate(rows):
        passed, extra = row_filter_cache[row_idx]
        if not passed:
            row_score_cache.append(None)
            continue
        market_cap = extra.get("market_cap", 0)
        sole_source_raw = row.get("sole_source", "")
        sole_source = sole_source_raw.strip().lower() in ("true", "1", "yes")
        contract = {
            "awardee_name": row.get("awardee_name", ""),
            "award_amount": float(row.get("award_amount", 0) or 0),
            "sole_source": sole_source,
            "agency": row.get("agency", ""),
            "naics": row.get("naics", ""),
        }
        has_pr = extra.get("has_press_release", False)
        prior_wins = extra.get("agency_prior_win_count", 0)
        is_first_agency = (prior_wins == 0)
        raw_score, _ = score_contract(contract, market_cap,
                                      threshold=0, has_press_release=has_pr,
                                      is_first_agency_win=is_first_agency)
        row_score_cache.append((raw_score, extra))

    # Compute date range for temporal metrics
    dates = [normalize_date(r.get("posted_date", "")) for r in rows]
    dates = [d for d in dates if d]
    if len(dates) >= 2:
        from datetime import datetime as _dt
        date_range_days = (_dt.strptime(max(dates), "%Y-%m-%d")
                           - _dt.strptime(min(dates), "%Y-%m-%d")).days
        date_range_days = max(date_range_days, 1)
    else:
        date_range_days = None

    combos = list(itertools.product(
        PARAM_GRID["score_threshold"],
        PARAM_GRID["take_profit_pct"],
        PARAM_GRID["stop_loss_pct"],
        PARAM_GRID["max_hold_days"],
    ))
    log.info(f"Testing {len(combos)} parameter combinations (fully offline)")

    best_score = -999
    best_combo = None
    opt_rows = []

    for combo_idx, (threshold, tp, sl, hold) in enumerate(combos):
        if sl >= tp:
            continue

        trades = []
        seen_trades = set()
        for row_idx, row in enumerate(rows):
            cached = row_score_cache[row_idx]
            if cached is None:
                continue
            raw_score, extra = cached
            if raw_score < threshold:
                continue

            ticker = row.get("ticker", "")
            award_date = row.get("posted_date", "")[:10]
            key = (ticker, award_date)
            if key in seen_trades:
                continue
            seen_trades.add(key)

            sim = simulate_trade_from_row(row, tp, sl, hold)
            if sim:
                trades.append({
                    "pnl":  sim["pnl_pct"],
                    "peak": sim.get("peak_pnl_pct", 0),
                })

        stats = _stats(trades, tp, sl, threshold, hold, date_range_days)
        opt_rows.append(stats)

        combo_score = _rank_score(stats)
        if combo_score > best_score:
            best_score = combo_score
            best_combo = stats

        if (combo_idx + 1) % 200 == 0:
            log.info(f"  [{combo_idx+1}/{len(combos)}] combos tested, "
                     f"best SQN so far: {best_score:.3f}")
```

- [ ] **Update `optimize_from_cache`** to remove mcap filtering and fix trades dict:

In the per-combo loop, replace:
```python
# Apply market cap filter
try:
    row_mcap = float(row.get("market_cap", 0) or 0)
except (ValueError, TypeError):
    row_mcap = 0
if row_mcap > 0 and row_mcap > mcap_limit:
    continue
```
with nothing (delete it).

Change `trades.append(sim["pnl_pct"])` to:
```python
trades.append({"pnl": sim["pnl_pct"], "peak": sim.get("peak_pnl_pct", 0)})
```

Remove `mcap_limit` from the combo tuple and PARAM_GRID iteration.

Update `_stats` call to remove `mcap_limit` arg and add `date_range_days=None`.

- [ ] **Fix `_print_top10` label** on line ~376: change  
`">>> BEST COMBO (highest total % return):"` → `">>> BEST COMBO (highest SQN score):"`

- [ ] **Update `_write_opt_results`** to sort by SQN descending before writing:

```python
def _write_opt_results(rows):
    if not rows:
        return
    rows_sorted = sorted(rows, key=lambda r: _rank_score(r), reverse=True)
    fields = list(rows_sorted[0].keys())
    with open(OPT_RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_sorted)
    log.info(f"Optimizer results written to {OPT_RESULTS_FILE}")
```

---

### Task 4: Update GUI — remove mcap, add new stat cards

**Files:**
- Modify: `gui.py`

- [ ] **Update `OptimizerTab.apply_params` signal** — remove the 5th `int` (max_mcap_m):
```python
apply_params = pyqtSignal(float, float, int, int)
```

- [ ] **Update `BacktestTab.apply_optimizer_params`** — remove `max_mcap_m` param and the two lines that use it:
```python
def apply_optimizer_params(self, tp: float, sl: float, hold: int, threshold: int):
    """Called from OptimizerTab to push best params here."""
    self._mode_combo.setCurrentIndex(1)
    self._tp.setValue(tp)
    self._sl.setValue(sl)
    self._hold.setValue(hold)
    self._threshold.setValue(threshold)
    self._on_mode_change(1)
```
Also remove `self._optimizer_max_mcap_m = 500` from `__init__`.

- [ ] **Remove `--max-market-cap` from `BacktestTab._run_backtest`** args list (the two lines passing `"--max-market-cap"` and the value).

- [ ] **Remove max_mcap_m from `BacktestTab._load_optimizer_params`** — delete the line:
```python
try: self._optimizer_max_mcap_m = int(float(best.get("max_mcap_M", 500)))
```

- [ ] **Replace `_load_results` stat cards in `OptimizerTab`** with the new layout. Replace from `# Row 1: param cards` to `self._apply_btn.setEnabled(True)` with:

```python
        # Row 1: optimized parameters
        row1 = QHBoxLayout(); row1.setSpacing(6)
        def _pct(key, default=""):
            v = best.get(key, default)
            try: return f"{float(v):.1f}%"
            except Exception: return str(v)
        def _val(key, default="—"):
            v = best.get(key, default)
            return str(v) if v not in (None, "", "None") else "—"

        for label, value, color in [
            ("Score Threshold", _val("score_threshold"), "#89b4fa"),
            ("Take Profit",     _pct("tp_pct"),          "#a6e3a1"),
            ("Stop Loss",       _pct("sl_pct"),           "#f38ba8"),
            ("Hold Days",       _val("max_hold_days"),    "#cdf4f4"),
        ]:
            row1.addWidget(_make_stat_card(label, value, color))
        self._best_lay.addLayout(row1)

        # Row 2: trade count and frequency
        row2 = QHBoxLayout(); row2.setSpacing(6)
        trades_n = int(float(best.get("trades", 0)))
        tpw = float(best.get("trades_per_week", 0))
        total_pnl = float(best.get("total_pnl_pct", 0))
        pnl_pw = float(best.get("avg_pnl_per_week", 0))
        for label, value, color in [
            ("Trades",             str(trades_n),              "#cdd6f4"),
            ("Trades / Week",      f"{tpw:.2f}",               "#cdd6f4"),
            ("Total Return",       f"{total_pnl:+.2f}%",       "#a6e3a1" if total_pnl >= 0 else "#f38ba8"),
            ("Avg PnL / Week",     f"{pnl_pw:+.2f}%",          "#a6e3a1" if pnl_pw >= 0 else "#f38ba8"),
        ]:
            row2.addWidget(_make_stat_card(label, value, color))
        self._best_lay.addLayout(row2)

        # Row 3: per-trade quality metrics
        row3 = QHBoxLayout(); row3.setSpacing(6)
        wr = float(best.get("win_rate", 0))
        avg_pnl = float(best.get("avg_pnl_pct", 0))
        avg_win = float(best.get("avg_win", 0))
        avg_loss = float(best.get("avg_loss", 0))
        sqn = _rank_score_gui(best)
        for label, value, color in [
            ("Win Rate",      f"{wr:.1f}%",           "#a6e3a1" if wr >= 50 else "#f38ba8"),
            ("Avg PnL/Trade", f"{avg_pnl:+.3f}%",     "#a6e3a1" if avg_pnl >= 0 else "#f38ba8"),
            ("Avg Win",       f"+{avg_win:.3f}%",      "#a6e3a1"),
            ("Avg Loss",      f"-{avg_loss:.3f}%",     "#f38ba8"),
            ("SQN Score",     f"{sqn:.2f}" if sqn > -999 else "n/a", "#f9e2af"),
        ]:
            row3.addWidget(_make_stat_card(label, value, color))
        self._best_lay.addLayout(row3)

        self._apply_btn.setEnabled(True)
        self._view_all_btn.setEnabled(True)
```

- [ ] **Add `_rank_score_gui` helper** near the top of the `OptimizerTab` class section (just before the class definition, ~line 1431) so the GUI can compute SQN without importing optimizer.py:

```python
def _rank_score_gui(row: dict) -> float:
    """Compute SQN for display. Mirrors optimizer._rank_score."""
    import math
    n = int(float(row.get("trades", 0)))
    if n < 15:
        return -999.0
    exp = float(row.get("expectancy", 0))
    std = float(row.get("std_dev_pnl", 0))
    if std <= 0:
        return -999.0
    return (exp / std) * math.sqrt(min(n, 100))
```

- [ ] **Update `_apply_best`** — remove `max_mcap_m` and fix the signal emit:
```python
def _apply_best(self):
    b = self._best_row
    if not b:
        return
    try:
        tp   = float(b.get("tp_pct", 8.0)) / 100
        sl   = float(b.get("sl_pct", 7.0)) / 100
        hold = int(float(b.get("max_hold_days", 4)))
        thr  = int(float(b.get("score_threshold", 40)))
        self.apply_params.emit(tp, sl, hold, thr)
    except Exception:
        pass
```

- [ ] **Update `_load_results` sort** to use SQN instead of total_pnl_pct:
```python
try:
    rows.sort(key=lambda r: _rank_score_gui(r), reverse=True)
except Exception:
    pass
```

- [ ] **Update the `apply_optimizer_params` connection** in the main window wiring. Search for where `apply_params` signal is connected to `apply_optimizer_params` and confirm the slot signature matches (both now take 4 args: tp, sl, hold, threshold).

---

### Task 5: Smoke test

- [ ] Run optimizer on a small date window to confirm it completes without error:
```bash
rtk python optimizer.py from-training-csv datasets/training_set_final.csv --start 2023-01-01 --end 2023-12-31 --quiet
```
Expected: runs to completion, prints top-10 table, writes `optimizer_results.csv`. No `max_market_cap` or `max_mcap_M` column in output.

- [ ] Verify `optimizer_results.csv` has new columns: `std_dev_pnl`, `trades_per_week`, `avg_pnl_per_week`.

- [ ] Verify `max_mcap_M` column is absent from `optimizer_results.csv`.

- [ ] Launch GUI and confirm the optimizer panel shows all three rows of stat cards without error:
```bash
rtk python gui.py
```

---
