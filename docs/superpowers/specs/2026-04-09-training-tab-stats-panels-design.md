# Training Data Tab — Stage Stats Panels

**Date:** 2026-04-09

## Summary

Add a stats panel below the run button in each of the three stage columns in the Training Data tab. Stats are read from checkpoint JSON files on disk and refresh every time a stage completes or the dataset selector changes.

## Layout

```
[Dataset selector row]
[stages_row HBoxLayout — 3 equal columns]
  ┌─ Stage 1 ──────────┐  ┌─ Stage 2 ──────────┐  ┌─ Stage 3 ──────────┐
  │ file label          │  │ file label          │  │ file label          │
  │ filter criteria     │  │ tiers info          │  │ data info           │
  │ status label        │  │ status label        │  │ status label        │
  │ [Run Build btn]     │  │ [Resume Build btn]  │  │ [Enrich OHLC btn]  │
  │ ─────────────────   │  │ ─────────────────   │  │ ─────────────────   │
  │ [Stats QLabel]      │  │ [Stats QLabel]      │  │ [Stats QLabel]      │
  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
[Progress bar]
[Output Log GroupBox — full width]
```

Each `[Stats QLabel]` is a `QLabel` using HTML `<pre>` monospace text, word-wrap off, styled with a subtle background, updated in-place on refresh.

## Stats Content

### Stage 1 (`stage1_filter.json`)

```
Total rows read:   13,013
─────────────────────────
Removed (IDV):        892  ( 6.9%)
Removed (amount):     256  ( 2.0%)
─────────────────────────
→ Into Stage 2:    11,865  (91.2%)
```

Requires adding `rows_removed_idv` and `rows_removed_amount` to the stage1 checkpoint in `build_training_set.py`. These are computed inside the `read_sam_gov_csv` reader loop.

### Stage 2 (`stage2_tickers.json`)

```
Total awards:      11,865
Resolved:           1,456  (12.3%)
Unresolved:        10,409  (87.7%)

By confidence (resolved):
  High:               193  (13.2%)
  Medium:           1,177  (80.8%)
  Low-medium:         402
  Low:             10,093

By evidence type:
  SEC exact:        1,263
  Known alias:        193
  Non-public:         622
  Low score:        3,917
  No match:         5,870
─────────────────────────
→ Into Stage 3:     1,456
```

Stats are computed by iterating the checkpoint dict (all entries loaded into memory — already is at ~12k entries).

### Stage 3 (`stage3_enrich.json`)

```
Qualifying (has ticker):  1,456
Enriched in checkpoint:   1,456  (100%)

With OHLC prices:         1,234  (84.7%)
With 8-K filing:            345  (23.7%)
With dilutive filing:        89   (6.1%)
With hist market cap:     1,100  (75.5%)
─────────────────────────
→ Final dataset:          1,456
```

"With OHLC" = `price_t0 != ""`. Other counts check non-empty fields.

## Changes Required

### 1. `build_training_set.py` — Stage 1 filter tracking

In `stage1_load_and_filter()`:
- Track `rows_removed_idv` and `rows_removed_amount` separately during the read loop (requires moving some filter logic out of the reader or adding counters in the stage function).
- Save both counters to the stage1 checkpoint.

The `read_sam_gov_csv` reader currently filters at read-time. The stage function needs to count rejections. Options:
- Pass a stats accumulator dict into the reader, or
- Move the filter check into `stage1_load_and_filter` with a two-pass approach (simpler: modify reader to emit all rows including rejected ones with a flag, then count in stage1).

Simplest: add a `rejection_stats` dict to the `ContractRecord` reader and return aggregate counts alongside the records.

### 2. `gui.py` — Stats panels

- Add `_s1_stats`, `_s2_stats`, `_s3_stats` `QLabel` widgets to each stage group box (after the run button).
- Add `_refresh_stats()` method that reads the checkpoint files for the currently selected dataset and populates the three labels.
- Call `_refresh_stats()` from `_refresh_status()` and `_on_finished()`.
- Stats label shows "No data yet" (grayed out) when checkpoint is missing.

## Refresh Triggers

| Event | Action |
|-------|--------|
| Tab load / `__init__` | `_refresh_status()` → `_refresh_stats()` |
| Dataset combo change | `_refresh_status()` → `_refresh_stats()` |
| Stage completes (`_on_finished`) | `_refresh_stats()` |
| File watcher triggers | `_refresh_status()` → `_refresh_stats()` |

## Error Handling

- If checkpoint file is missing or unreadable: show "No data yet" in gray.
- If checkpoint has unexpected schema: show what's available, skip missing fields.
- No crashing — all stat reads are wrapped in try/except.
