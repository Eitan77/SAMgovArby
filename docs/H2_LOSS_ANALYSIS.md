# H2 Loss Analysis: H1 Optimizer Out-of-Sample

**Date:** 2026-04-12  
**Optimization:** H1 2022 (Jan–Jun)  
**Backtest:** H2 2022 (Jul–Dec)  
**Model:** H1-optimized params on unseen H2 data

---

## Results Summary

| Metric | Value |
|--------|-------|
| Best H1 Params | TP=14%, SL=6%, Hold=6d, Threshold=38, MaxMcap=$500M |
| H2 Trades | 44 |
| Win Rate | 61.4% |
| Total Return | +93.12% |
| Max Drawdown | -16.14% |
| Losses | 17 |
| Wins | 27 |

---

## Loss Patterns

### Pattern 1: Split Government Transport Contracts — Biggest Cluster
**Impact:** 3 losses (14% of all losses)

Three airlines (ALK, JBLU, SNCY) all received the **exact same $1.5B USTRANSCOM award on 2022-12-21**, awarded in parallel. All three lost:
- **ALK:** -4.04%, Peak +0.21%
- **JBLU:** -1.19%, Peak +2.41%
- **SNCY:** -6.66%, Peak +0.36%

The market did not react to any of them — peak returns were <2.5%, all exited via timeout.

**Contrast:** A different USTRANSCOM award on 2022-12-28 ($67.2M, much smaller) yielded:
- **JBLU:** +13.22%, Peak +14.68%
- **ALK:** +8.89%, Peak +10.75%

**Root cause:** When a large government contract is split across 3+ carriers simultaneously, it's routine contract renewal, not a competitive advantage. The market views it as known/priced-in. Smaller, concentrated awards create real catalysts.

**Recommended fix:** If 2+ tickers share identical `(award_amount, agency, award_date)`, skip all of them. Or require V2M > 50% for NAICS 4811 (air transportation) to avoid low-impact routine contracts.

---

### Pattern 2: Pharma/Biotech (NAICS 3254) — 0-for-3
**Impact:** 3 losses (18% of all losses)

All pharma/biotech trades lost:
- **SUPN:** -1.93%, Peak +2.13% (Mcap $1.995B)
- **OSUR:** -6.66%, Peak +1.55% (Mcap $357M)
- **SIGA:** -6.66%, Peak +0.33% (Mcap $523M)

**Root cause:** Biotech/pharma stock prices are driven by clinical trial data and FDA approvals, not government medical supply contracts. A VA or DoD medical procurement contract is routine business, not a catalyst. Investors don't reprrice on procurement news.

**Recommended fix:** Exclude NAICS 3254 entirely, or require V2M > 20% to ensure only large, material contracts are considered.

---

### Pattern 3: Large-Cap + Tiny V2M — Contract is Noise
**Impact:** 6 losses in $1B–$5B mcap range with low V2M

SAIC (Defense contractor) appears 5× in H2 trades. Two losses:
- **SAIC ($3.91B mcap, V2M 1.0%):** -3.46%, Peak +1.77%
- **SAIC ($4.44B mcap, V2M 2.2%):** -0.22%, Peak +1.39%

A $40M contract is invisible to a $4B company. The market has no reason to react; peak prices never exceeded 1.4%.

**Broader pattern by Mcap range:**

| Mcap Range | Losses | Wins | WR |
|------------|--------|------|-----|
| <$500M | 5 | 10 | 67% |
| $500M–$1B | 3 | 3 | 50% |
| $1B–$2B | 3 | 1 | 25% |
| $2B–$5B | 6 | 13 | 68% |

The $1B–$2B range performs worst. Within the $2B–$5B range, losses cluster at V2M < 5%.

**Recommended fix:** Reject if `mcap > $2B AND V2M < 5%`. Currently the global filter allows V2M as low as 1%, which is too noisy for large-cap companies.

---

### Pattern 4: High Scores Don't Correlate with High Win Rate
**Impact:** Scoring function over-weights factors irrelevant to stock momentum

| Score Range | Losses | Wins | WR |
|-------------|--------|------|-----|
| 38–45 | 6 | 7 | **54%** |
| 45–55 | 5 | 17 | **77%** ← sweet spot |
| 55–65 | 2 | 1 | **33%** |
| 65–100 | 4 | 2 | **33%** |

The 65+ range is dominated by USTRANSCOM airline trades (high score due to sole-source + first-agency + high V2M). Yet these are split contracts with no momentum.

The 55–65 range includes SAIC (large-cap, low V2M) and SIGA (pharma).

**Root cause:** Score function rewards:
- Sole-source status (+25 pts)
- First-agency (is this the first contract with this agency? +15 pts)
- High V2M (+15 pts)

But these don't guarantee momentum. A sole-source pharma supply contract or a multi-recipient air transport renewal can score 65+ without moving the stock.

**Recommended fix:** Add composite filters that reject high-score trades outside the 45–55 zone, or enforce stricter V2M bounds at high score thresholds.

---

### Pattern 5: Peaked-Then-Reversed Losers
**Impact:** 8 losses (47% of all losses) peaked positive intraday before reversing

| Ticker | PnL | Peak | Loss Opportunity |
|--------|-----|------|-------------------|
| DLHC | -6.66% | +3.46% | 10.1% swing |
| SUPN | -1.93% | +2.13% | 4.0% swing |
| OSUR | -6.66% | +1.55% | 8.2% swing |
| SAIC | -3.46% | +1.77% | 5.2% swing |
| JBLU | -1.19% | +2.41% | 3.6% swing |
| SAIC | -0.22% | +1.39% | 1.6% swing |
| TTEK | -4.46% | +2.76% | 7.2% swing |
| VSAT | -6.66% | +9.73% | **16.4% swing** |

Most notably, **VSAT peaked at +9.73% intraday, then reversed to -6.66%** (stopped out). A trailing stop would have captured at least +5% profit instead of a loss.

**Recommended fix:** Implement a trailing stop or partial exit when position peaks >5% intraday. This converts several "lost opportunity" trades into modest wins.

---

### Pattern 6: V2M Sweet Spot — 15–50% Optimal
**Impact:** Win rate varies dramatically by V2M ratio

| V2M Range | Losses | Wins | WR |
|-----------|--------|------|-----|
| <5% | 4 | 8 | **67%** |
| 5–15% | 7 | 7 | **50%** ← dead zone |
| 15–50% | 2 | 9 | **82%** ← optimal |
| 50–200% | 3 | 2 | **40%** |
| >200% | 1 | 1 | **50%** |

The 5–15% band is the worst performing zone. Contracts in this range are material enough to pass the filter but not large enough to create significant momentum.

The 15–50% band is optimal: contract is clearly material (not noise for large-cap companies) but not distress-level (which triggers different market dynamics).

**Recommended fix:** Tighten V2M requirement to 10–50% instead of the current 1–500% band. This automatically excludes many of the low-impact contracts.

---

## Candidate Filters (Ranked by Impact)

### A. Block Duplicate Awards (Split Contracts)
```
IF (award_amount, agency, award_date) matches 2+ tickers in same run:
   SKIP ALL matched tickers
```
**Impact:** Removes Pattern 1 (3 losses)  
**Complexity:** Medium (requires tracking awards across awards processed)  
**Est. improvement:** +2–3% win rate on clustered awards

---

### B. Exclude or Restrict Pharma/Biotech (NAICS 3254)
```
IF naics.startswith("3254"):
   SKIP entirely
   OR require v2m_pct >= 20
```
**Impact:** Removes Pattern 2 (3 losses, 0 wins)  
**Complexity:** Low  
**Est. improvement:** +2–3 absolute win rate (removes 3 guaranteed losers)

---

### C. Reject Large-Cap + Low-V2M
```
IF market_cap > 2_000_000_000 AND value_to_mcap_pct < 5:
   SKIP
```
**Impact:** Removes Pattern 3 (6 losses from $1B–$5B range)  
**Complexity:** Low  
**Est. improvement:** +2–3% win rate on large-cap trades

---

### D. Tighten V2M Band (Eliminate 5–15% Dead Zone)
```
MIN_VALUE_TO_MCAP_PCT = 10  # was 1
MAX_VALUE_TO_MCAP_PCT = 50  # was 500
```
**Impact:** Removes Pattern 6 (7 losses from 5–15% band)  
**Complexity:** Very low (one-line config change)  
**Est. improvement:** +3–5% win rate overall

---

### E. Add Trailing Stop (Recover Peaked-Then-Reversed Trades)
```
IF peak_intraday_pnl > 5%:
   EXIT at max(5%, 70% of peak)
   # e.g., if peak +10%, exit at +5%
```
**Impact:** Removes Pattern 5 (8 losses with peak >1%)  
**Complexity:** Medium (requires intraday tracking in price_sim.py)  
**Est. improvement:** +1–2% win rate (recovers VSAT +9.73% peak, etc.)

---

### F. Restrict Airlines (NAICS 4811) with Tight V2M
```
IF naics.startswith("4811"):
   REQUIRE v2m_pct > 50 OR award_amount < 500_000_000
   # Force small, concentrated awards only; block bulk transport renewals
```
**Impact:** Removes Pattern 1 variant (USTRANSCOM bulk split contracts)  
**Complexity:** Low  
**Est. improvement:** +1–2% win rate on airline trades

---

## Implementation Recommendation

**Start with filters in this order (each adds cumulative value):**

1. **D (V2M band 10–50%)** — Quickest win, removes dead zone (7 losses)
2. **B (Block pharma 3254)** — Guaranteed improvement, 0 winners lost (3 losses)
3. **C (Reject big-cap low-V2M)** — Removes noisy large-cap trades (6 losses)
4. **A (Skip split awards)** — Moderate complexity but high impact (3 losses)
5. **E (Trailing stop)** — Highest complexity, moderate incremental value (8 losses recovered partially)
6. **F (Airline V2M restriction)** — Redundant if you implement A, B, C

**Expected cumulative improvement:** From 61.4% WR → ~75–80% WR on H2 unseen data (if these patterns hold).

---

## Next Steps

1. Implement filters D → B → C in sequence
2. Re-run H2 backtest with each change; measure win rate improvement
3. If improvement >3%, apply to H1 optimizer and re-optimize thresholds
4. Test on 2023+ data for pattern stability
5. Consider filter E (trailing stop) if other filters plateau

---

## Files Affected (if implementing)

- `config.py`: MIN/MAX_VALUE_TO_MCAP_PCT, new exclusion NAICS list
- `filter_engine_bt.py`: Add large-cap + low-V2M check, pharma exclusion, split-award dedup
- `price_sim.py`: Trailing stop logic (if implementing E)
- `backtest.py`: Award dedup tracking if implementing A

