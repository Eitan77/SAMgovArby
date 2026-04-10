# Training Tab Stats Panels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-stage stats panels (below each run button) to the Training Data tab in the GUI, showing filter/resolution/enrichment breakdowns with row counts and percentages, refreshed after every stage run.

**Architecture:** (1) Enhance `sam_gov_reader.py` to collect per-criterion rejection counts via an optional stats dict. (2) Persist those counts into the stage1 checkpoint in `build_training_set.py`. (3) Add `QLabel` stats panels to each stage `QGroupBox` in `gui.py`, populated by reading checkpoint JSON files.

**Tech Stack:** Python 3, PyQt6, JSON checkpoints already on disk.

---

## File Map

| File | Change |
|------|--------|
| `sam_gov_reader.py` | Add optional `rejection_stats` param to `read_sam_gov_csv`; increment counters on each filtered row |
| `build_training_set.py` | Pass `rejection_stats` dict into reader; save new keys to stage1 checkpoint |
| `gui.py` | Add `_s1_stats`, `_s2_stats`, `_s3_stats` labels in stage group boxes; add `_refresh_stats()`; call it from `_refresh_status()` and `_on_finished()` |

---

## Task 1: Add rejection tracking to `sam_gov_reader.py`

**Files:**
- Modify: `sam_gov_reader.py:71-143`

- [ ] **Step 1: Update `read_sam_gov_csv` signature**

Replace the function signature and add counter increments at each `continue` statement:

```python
def read_sam_gov_csv(path: str, rejection_stats: dict | None = None) -> Iterator[ContractRecord]:
    """Read SAM.gov bulk CSV export, yield validated ContractRecord per row.

    Handles the SAM.gov report preamble by scanning for the header row
    (the line containing "CAGE Code").

    Silently skips: foreign entities, IDV umbrellas, out-of-range/unparseable amounts.
    If rejection_stats dict is provided, increments keys:
      rows_total, rows_foreign, rows_idv, rows_amount
    Raises: FileNotFoundError if path does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"SAM.gov CSV not found: {path}")

    if rejection_stats is not None:
        rejection_stats.setdefault("rows_total", 0)
        rejection_stats.setdefault("rows_foreign", 0)
        rejection_stats.setdefault("rows_idv", 0)
        rejection_stats.setdefault("rows_amount", 0)

    def _inc(key: str):
        if rejection_stats is not None:
            rejection_stats[key] = rejection_stats.get(key, 0) + 1

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
            _inc("rows_total")

            # Hard-reject: foreign / missing country
            country = (row.get("Country of Incorporation") or "").strip().upper()
            if country != "USA":
                _inc("rows_foreign")
                continue

            # Hard-reject: IDV umbrella contracts
            if (row.get("IDV Type") or "").strip():
                _inc("rows_idv")
                continue

            # Hard-reject: unparseable or out-of-range amount
            try:
                amount = float((row.get(_AMOUNT_COL) or "0").replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                _inc("rows_amount")
                continue
            if amount < MIN_CONTRACT_VALUE or amount > MAX_AWARD_AMOUNT:
                _inc("rows_amount")
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
                posted_date=             _parse_date(row.get("Date Signed") or ""),
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
```

- [ ] **Step 2: Commit**

```bash
rtk git add sam_gov_reader.py
rtk git commit -m "feat: add rejection_stats tracking to read_sam_gov_csv"
```

---

## Task 2: Persist rejection counts in stage1 checkpoint

**Files:**
- Modify: `build_training_set.py:312-343` (inside `stage1_load_and_filter`)

- [ ] **Step 1: Pass `rejection_stats` into the reader loop**

In `stage1_load_and_filter`, replace the reader loop block (lines 312–343) with:

```python
    awards_by_key: dict[str, dict] = {}
    records_by_key: dict[str, ContractRecord] = {}
    rejection_stats: dict = {}

    for record in read_sam_gov_csv(sam_csv, rejection_stats=rejection_stats):
        if month_filter and record.posted_date:
            try:
                if int(record.posted_date.split("-")[1]) != month_filter:
                    continue
            except (ValueError, IndexError):
                pass

        awards_by_key[record.piid] = _record_to_award_dict(record)
        records_by_key[record.piid] = record

        if rejection_stats.get("rows_total", 0) % 50_000 == 0 and rejection_stats.get("rows_total", 0) > 0:
            log.info(f"  ... {rejection_stats['rows_total']:,} rows read, {len(awards_by_key):,} unique awards so far")

    awards = list(awards_by_key.values())
    after_load = len(awards)
    total_rows = rejection_stats.get("rows_total", 0)
    log.info(f"  {total_rows:,} rows read → {after_load:,} awards (amount/IDV filtered at read time)")
    log.info(f"  Final: {len(awards):,} contracts")

    _write_csv(FILTERED_CSV, awards)

    _save_cp(CP_STAGE1, {
        "total_rows_read":    total_rows,
        "rows_removed_foreign": rejection_stats.get("rows_foreign", 0),
        "rows_removed_idv":     rejection_stats.get("rows_idv", 0),
        "rows_removed_amount":  rejection_stats.get("rows_amount", 0),
        "after_load":         after_load,
        "final_count":        len(awards),
    })
```

Note: Remove the old `total_rows = 0` declaration and the old `total_rows += 1` / progress log that preceded this block.

- [ ] **Step 2: Commit**

```bash
rtk git add build_training_set.py
rtk git commit -m "feat: persist per-criterion rejection counts in stage1 checkpoint"
```

---

## Task 3: Add stats panels to Training Data tab in `gui.py`

**Files:**
- Modify: `gui.py:1630-1798` (TrainingDataTab class)

### Step 3a — Add a helper and stats label widgets to `_build_ui`

- [ ] **Step 1: Add `_make_stats_label` helper and insert stats labels in each group box**

Inside `_build_ui`, after `s1l.addWidget(self._btn_s1)` (currently line ~1685), add:

```python
        self._s1_stats = self._make_stats_label()
        s1l.addWidget(self._s1_stats)
```

After `s2l.addWidget(self._btn_s2)` (~line 1700):

```python
        self._s2_stats = self._make_stats_label()
        s2l.addWidget(self._s2_stats)
```

After `s3l.addWidget(self._btn_s3)` (~line 1716):

```python
        self._s3_stats = self._make_stats_label()
        s3l.addWidget(self._s3_stats)
```

Add `_make_stats_label` as a new method on the class:

```python
    @staticmethod
    def _make_stats_label() -> QLabel:
        lbl = QLabel("No data yet")
        lbl.setStyleSheet(
            "color: #a6adc8; font-size: 11px; font-family: monospace;"
            "background: #181825; border-radius: 4px; padding: 6px;"
        )
        lbl.setWordWrap(False)
        lbl.setTextFormat(Qt.TextFormat.RichText)
        return lbl
```

### Step 3b — Add `_refresh_stats` method

- [ ] **Step 2: Add `_refresh_stats` method to `TrainingDataTab`**

Add this method to the class (after `_refresh_status`):

```python
    def _refresh_stats(self):
        ds   = self._ds()
        tag  = ds["tag"]
        cpdir = DATASETS_DIR / f"checkpoints_{tag}"

        # ── Stage 1 ──────────────────────────────────────────────
        try:
            with open(cpdir / "stage1_filter.json") as f:
                s1 = json.load(f)
            total   = s1.get("total_rows_read", 0)
            foreign = s1.get("rows_removed_foreign", 0)
            idv     = s1.get("rows_removed_idv", 0)
            amount  = s1.get("rows_removed_amount", 0)
            passed  = s1.get("final_count", 0)
            def pct(n): return f"{n/total*100:5.1f}%" if total else "  — "
            self._s1_stats.setText(
                f"Total rows read : {total:>8,}<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"Removed (foreign): {foreign:>7,}  ({pct(foreign)})<br>"
                f"Removed (IDV)    : {idv:>7,}  ({pct(idv)})<br>"
                f"Removed (amount) : {amount:>7,}  ({pct(amount)})<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"<span style='color:#a6e3a1'>→ Into Stage 2   : {passed:>7,}  ({pct(passed)})</span>"
            )
        except Exception:
            self._s1_stats.setText("<span style='color:#585b70'>No data yet</span>")

        # ── Stage 2 ──────────────────────────────────────────────
        try:
            with open(cpdir / "stage2_tickers.json") as f:
                s2 = json.load(f)
            total2   = len(s2)
            resolved = sum(1 for v in s2.values() if v.get("ticker"))
            unres    = total2 - resolved
            def pct2(n): return f"{n/total2*100:5.1f}%" if total2 else "  — "
            def rpct(n): return f"{n/resolved*100:5.1f}%" if resolved else "  — "

            from collections import Counter
            confs  = Counter(v.get("ticker_confidence", "") for v in s2.values())
            evids  = Counter(v.get("evidence_type", "")     for v in s2.values())

            self._s2_stats.setText(
                f"Total awards     : {total2:>8,}<br>"
                f"Resolved         : {resolved:>8,}  ({pct2(resolved)})<br>"
                f"Unresolved       : {unres:>8,}  ({pct2(unres)})<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"By confidence (resolved):<br>"
                f"&nbsp;&nbsp;High       : {confs.get('high',0):>6,}  ({rpct(confs.get('high',0))})<br>"
                f"&nbsp;&nbsp;Medium     : {confs.get('medium',0):>6,}  ({rpct(confs.get('medium',0))})<br>"
                f"&nbsp;&nbsp;Low-medium : {confs.get('low_medium',0):>6,}  ({rpct(confs.get('low_medium',0))})<br>"
                f"&nbsp;&nbsp;Low        : {confs.get('low',0):>6,}<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"By evidence type:<br>"
                f"&nbsp;&nbsp;SEC exact  : {evids.get('sec_exact',0):>6,}<br>"
                f"&nbsp;&nbsp;Known alias: {evids.get('known_alias',0):>6,}<br>"
                f"&nbsp;&nbsp;Non-public : {evids.get('null:non_public_entity',0):>6,}<br>"
                f"&nbsp;&nbsp;Low score  : {evids.get('null:low_score',0):>6,}<br>"
                f"&nbsp;&nbsp;No match   : {evids.get('none',0):>6,}<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"<span style='color:#a6e3a1'>→ Into Stage 3   : {resolved:>7,}</span>"
            )
        except Exception:
            self._s2_stats.setText("<span style='color:#585b70'>No data yet</span>")

        # ── Stage 3 ──────────────────────────────────────────────
        try:
            with open(cpdir / "stage3_enrich.json") as f:
                s3 = json.load(f)
            enriched  = len(s3)
            # Stage 2 checkpoint needed for "qualifying" count
            try:
                with open(cpdir / "stage2_tickers.json") as f2:
                    s2_data = json.load(f2)
                qualifying = sum(1 for v in s2_data.values() if v.get("ticker"))
            except Exception:
                qualifying = enriched
            def pct3(n): return f"{n/qualifying*100:5.1f}%" if qualifying else "  — "

            with_ohlc    = sum(1 for v in s3.values() if v.get("price_t0") not in ("", None))
            with_8k      = sum(1 for v in s3.values() if v.get("first_8k_date") not in ("", None))
            with_dilutive= sum(1 for v in s3.values() if v.get("last_dilutive_filing_date") not in ("", None))
            with_mcap    = sum(1 for v in s3.values() if v.get("historical_market_cap_approx") not in ("", None))

            self._s3_stats.setText(
                f"Qualifying (ticker)  : {qualifying:>6,}<br>"
                f"Enriched             : {enriched:>6,}  ({pct3(enriched)})<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"With OHLC prices     : {with_ohlc:>6,}  ({pct3(with_ohlc)})<br>"
                f"With 8-K filing      : {with_8k:>6,}  ({pct3(with_8k)})<br>"
                f"With dilutive filing : {with_dilutive:>6,}  ({pct3(with_dilutive)})<br>"
                f"With hist market cap : {with_mcap:>6,}  ({pct3(with_mcap)})<br>"
                f"<span style='color:#45475a'>──────────────────────────</span><br>"
                f"<span style='color:#a6e3a1'>→ Final dataset  : {enriched:>7,}</span>"
            )
        except Exception:
            self._s3_stats.setText("<span style='color:#585b70'>No data yet</span>")
```

Note: `json` is not yet imported in `gui.py` — add `import json` to the imports at the top of the file.

### Step 3c — Wire refresh calls

- [ ] **Step 3: Call `_refresh_stats()` from `_refresh_status` and `_on_finished`**

In `_refresh_status` (currently ends after the for-loop at ~line 1756), add at the end:

```python
        self._refresh_stats()
```

In `_on_finished` (currently line ~1790):

```python
    def _on_finished(self, code: int):
        self._set_building(False)
        self._refresh_status()   # already calls _refresh_stats via _refresh_status
```

(`_refresh_status` already calls `_refresh_stats`, so no second call needed here — `_refresh_status()` in `_on_finished` is sufficient.)

- [ ] **Step 4: Add `import json` to gui.py imports**

At the top of `gui.py`, add `json` to the existing stdlib imports block:

```python
import csv
import json
import logging
```

- [ ] **Step 5: Commit**

```bash
rtk git add gui.py
rtk git commit -m "feat: add stage stats panels to Training Data tab"
```

---

## Task 4: Smoke test

- [ ] **Step 1: Verify stats panels render with existing checkpoint data**

```bash
rtk python gui.py
```

Open the Training Data tab. Each of the 3 stage columns should show a stats panel below the run button with real numbers from the H1/H2 checkpoints. Switching the dataset combo should update all three panels.

- [ ] **Step 2: Verify "No data yet" fallback**

Temporarily rename a checkpoint file and reopen the tab — the corresponding panel should show "No data yet" in gray. Rename it back.

- [ ] **Step 3: Final commit if any fixups were made**

```bash
rtk git add -p
rtk git commit -m "fix: training tab stats panels smoke test fixups"
```
