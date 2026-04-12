# Optimizer Audit — `optimizer.py`

_Audited: 2026-04-11 | Updated: 2026-04-12 — marked items fixed in audit session_

---

## How the Optimizer Works

### End-to-End Flow (training CSV mode — the primary path)

```
USASpending CSV
  └─ Stage 1: filter_training_set.csv     (IDIQ removed, $1M–$10B range)
  └─ Stage 2: stage2_with_tickers.csv     (TickerResolverV4, deduped by entity)
  └─ Stage 3: training_set_final.csv      (OHLC t0–t7, hist mcap, 8-K, dilutive, PR dates)
        │
        ▼
  optimizer.py: optimize_from_training_csv()
        │
        ├─ Pre-filter cache: apply_filters_bt_from_training(row) for every row
        │     Checks: min contract value, ticker confidence, hist mcap > 0,
        │             8-K within 2d of award, dilutive filing within 60d before award
        │
        ├─ Grid search: 9 × 6 × 5 × 6 × 6 = 9,720 combos
        │     score_threshold × take_profit % × stop_loss % × hold_days × max_mcap
        │     (combos where SL >= TP are skipped)
        │
        ├─ Per combo, for each passing row:
        │     1. Apply per-combo mcap_limit
        │     2. Re-score with score_contract() using combo's threshold
        │     3. Dedup by (ticker, posted_date)
        │     4. Simulate via simulate_trade_from_row() (stored OHLC — zero API calls)
        │
        ├─ _stats(): compute metrics per combo
        └─ _rank_score(): select best combo
             → optimizer_results.csv (all combos)
             → stdout: top 10 by total return + best combo summary
```

### What `_stats()` Returns Per Combo

| Field | Description |
|---|---|
| `trades` | Number of trades simulated |
| `win_rate` | % of trades with positive PnL |
| `avg_pnl_pct` | Arithmetic mean PnL across all trades |
| `total_pnl_pct` | Sum of all trade PnLs (not compound) |
| `expectancy` | `(win_rate × avg_win) - (loss_rate × avg_loss)` |
| `profit_factor` | Gross wins / gross losses (capped at 99) |
| `avg_win` / `avg_loss` | Mean PnL of winning / losing trades |
| `avg_peak_pnl` | Mean intraday peak PnL (MFE per trade) |
| `avg_return_t7` | **Always 0** — see Bug #1 |
| `peak_pnl_pct` | Best single trade's final PnL (not MFE) |
| `max_drawdown_pct` | Sequential drawdown on unsorted PnL stream — see Bug #4 |

### `_rank_score()` — Selection Formula (updated 2026-04-12)

```python
avg_pnl_per_week    # trades_per_week × avg_pnl_pct; requires N≥5
```

Selects the combo that maximizes weekly return, requiring a minimum of 5 trades to qualify. GUI uses the same metric (`_rank_score_gui`). Old SQN formula (avg_pnl × √n, N≥15) was replaced.

---

## Bugs

### Bug 1 — `avg_return_t7` is always 0 (silent dead metric)

**File:** `optimizer.py:232–236`, `price_sim.py:290–310`

`optimize_from_training_csv` collects `sim.get("return_t7", 0)` from `simulate_trade_from_row()`. But `_result()` in `price_sim.py` never sets a `"return_t7"` key. The key is never populated anywhere in `simulate_trade` or `simulate_trade_from_row`. Every row gets 0. The column appears in `optimizer_results.csv` and the summary print but is always 0 — useless and misleading.

**Fix:** Either compute `return_t7` in `_result()` from stored `close_t7` relative to entry, or remove the field entirely from `_stats`, the trades list, and the print output.

---

### Bug 2 — ~~`_rank_score` comment/label says "expectancy" but uses `avg_pnl`~~ ✅ FIXED 2026-04-12

**Fixed:** `_rank_score` now uses `avg_pnl_per_week` with N≥5 minimum. Print labels updated to "highest avg PnL/week". GUI `_rank_score_gui` synced to same formula.

---

### Bug 3 — Score re-computed per combo instead of per row (major performance waste)

**File:** `optimizer.py:202–219`

Inside the inner loop, `score_contract()` is called for every row × every combo. But the score value depends only on row-level inputs (`award_amount`, `market_cap`, `sole_source`, `agency`, `naics`, `has_pr`, `is_first_agency_win`) — none of which change between combos. The only combo parameter that affects the pass/fail decision is `score_threshold`.

This means `score_contract()` is called up to `n_rows × 9720` times when `n_rows × 1` calls would suffice. Pre-compute `score` once per row into a `row_score_cache`, then compare `score >= threshold` per combo.

**Current:** O(rows × combos) score calls  
**Should be:** O(rows) score calls + O(rows × combos) threshold comparisons

---

### Bug 4 — Max drawdown computed on unsorted (non-chronological) trade stream

**File:** `optimizer.py:328–338`

The `max_drawdown_pct` calculation walks the `pnls` list sequentially and computes cumulative drawdown. But `pnls` is built by iterating `rows` in CSV order, which is not chronological — it's ordered by entity position in the training file. Drawdown is a path-dependent metric; it only makes sense on a time-ordered series. The reported `max_drawdown_pct` is therefore a function of row order, not time, and is unreliable.

**Fix:** Sort rows by `posted_date` before collecting trades, or label this metric "sequential drawdown (not time-ordered)" so it's not confused with a proper drawdown figure.

---

### Bug 5 — `optimize_from_cache` silently drops peak and t7 metrics

**File:** `optimizer.py:111–112`

In the cache mode, trades are appended as raw floats: `trades.append(sim["pnl_pct"])`. The `_stats` function handles this via backward-compat check (`isinstance(t, (int, float))`), but `peaks` and `returns_t7` default to 0 for all trades. The resulting CSV shows `avg_peak_pnl = 0` and `avg_return_t7 = 0` for every cache-mode combo. There's no warning that these columns are empty.

**Fix:** In `optimize_from_cache`, collect the full dict like the training CSV path does: `trades.append({"pnl": sim["pnl_pct"], "peak": sim.get("peak_pnl_pct", 0), "return_t7": 0})`.

---

### Bug 6 — `optimize_from_api` imports `tempfile` but never uses it

**File:** `optimizer.py:260`

```python
import tempfile   # dead import
```

Remove it.

---

## Architecture Issues

### Issue A — ~~Grid search has no minimum trade count filter~~ ✅ FIXED 2026-04-12

**Fixed:** `_rank_score` now returns -999 for N < 5 (was N < 15 for old SQN). Combos with fewer than 5 trades are excluded from best-combo selection.

---

### Issue B — Score is re-evaluated from scratch each combo instead of cached

(See Bug 3 — listed as a bug due to severity, but it's fundamentally an architecture decision that blows up runtime.)

---

### Issue C — Pre-filter cache assumes config.MAX_MARKET_CAP ($5B) as ceiling

**File:** `optimizer.py:181`

```python
passed, _, extra = apply_filters_bt_from_training(row)  # uses config.MAX_MARKET_CAP
```

The cache pre-filters at the config-level $5B cap. Per-combo mcap cutoffs then further restrict. This is correct, but silently depends on `config.MAX_MARKET_CAP` being >= all combo values. If someone lowers `MAX_MARKET_CAP` in config to e.g. $500M, rows between $500M and $1B (which are in the combo grid) will be incorrectly pre-filtered out. 

**Fix:** Call `apply_filters_bt_from_training(row, max_market_cap=max(PARAM_GRID["max_market_cap"]))` explicitly, so the pre-filter ceiling is always the grid maximum, not the config default.

---

### Issue D — ~~Dual ranking systems (total return vs rank score) are inconsistent~~ ✅ FIXED 2026-04-12

**Fixed:** `_print_top10` now sorts by `avg_pnl_per_week` (same as `_rank_score`). Best combo and top-10 table use the same metric. GUI `_rank_score_gui` synced.

---

### Issue E — No persistence of combo progress (no checkpointing)

With ~9,720 combos, a 60-second-per-combo run (in cache mode with yfinance calls) would take 162 hours. Even the offline training CSV mode can take minutes with large datasets. There's no checkpoint — if the process crashes or is killed, all progress is lost.

**Recommendation:** For the `from-training-csv` mode, periodically write `opt_rows` to the CSV (e.g. every 500 combos) so partial results are recoverable. Add a `--resume` flag that loads an existing partial CSV and skips already-completed combos.

---

### Issue F — `normalize_date` duplicated between optimizer and price_sim

`normalize_date()` exists in `optimizer.py:26–40`. Nearly identical logic lives inside `simulate_trade_from_row()` in `price_sim.py:223–228`. Both handle M/D/YYYY → YYYY-MM-DD conversion. This should live in one place (e.g. a shared `utils.py` or `config.py`).

---

### Issue G — `optimize_from_cache` makes redundant yfinance calls

In cache mode, `simulate_trade(ticker, award_date, tp, sl, hold)` is called per row per combo. The OHLC data fetched from yfinance is identical across all combos for the same (ticker, date) — only the TP/SL/hold thresholds change. The function re-downloads the same price history ~9,720 times per ticker. There is no in-memory OHLC cache.

**Recommendation:** Pre-fetch and cache the OHLC DataFrame per (ticker, date) before the combo loop, then re-use stored data for simulation. This is already solved in the training CSV path via stored columns — which is another argument for preferring the training CSV mode over the cache mode.

---

### Issue H — `_write_opt_results` does not sort output

The CSV is written in evaluation order (not sorted by any metric). Analysts loading the file directly must sort manually. Writing sorted by `total_pnl_pct DESC` or `_rank_score DESC` would make the raw output immediately useful.

---

## Summary Table

| # | Severity | Type | Status | Description |
|---|---|---|---|---|
| Bug 1 | High | Bug | **Open** | `avg_return_t7` always 0 — metric is dead |
| Bug 2 | Medium | Bug | ✅ Fixed | `_rank_score` mislabeled — now uses `avg_pnl_per_week` |
| Bug 3 | High | Performance | **Open** | Score re-computed per combo instead of per row (O(n×combos) vs O(n)) |
| Bug 4 | Medium | Bug | **Open** | Max drawdown computed on unsorted stream — unreliable |
| Bug 5 | Low | Bug | **Open** | Cache mode silently zeroes peak/t7 metrics |
| Bug 6 | Low | Bug | **Open** | Dead `import tempfile` in `optimize_from_api` |
| Issue A | Medium | Architecture | ✅ Fixed | Min trade count — N≥5 now required for ranking |
| Issue B | — | (same as Bug 3) | **Open** | — |
| Issue C | Medium | Architecture | **Open** | Pre-filter ceiling tied to `config.MAX_MARKET_CAP` instead of grid max |
| Issue D | Medium | Architecture | ✅ Fixed | Dual ranking systems consolidated to `avg_pnl_per_week` |
| Issue E | Low | Architecture | **Open** | No checkpointing — full restart on crash |
| Issue F | Low | Architecture | **Open** | `normalize_date` duplicated in two files |
| Issue G | High | Performance | **Open** | yfinance re-downloaded per combo in cache mode |
| Issue H | Low | UX | **Open** | `optimizer_results.csv` written unsorted |
