# Ticker Resolver V4 — Deep Audit

**Date:** 2026-04-06
**Current Resolution Rate:** 96/810 = **11.9%**
**Target:** 30-40% (realistic given the mix of private contractors)

---

## Executive Summary

The resolver is leaving **massive** resolution on the table. The 571 `no_match` entities include companies like RAYTHEON, AECOM subsidiaries, and dozens of identifiable public-company subsidiaries. The root causes are:

1. **CAGE->GLEIF path is dead** (0.25% hit rate)
2. **EDGAR map has wrong tickers** (Boeing -> BA-PA instead of BA)
3. **No parent company resolution** (subsidiaries never match)
4. **Former/renamed companies never match** (Raytheon Company != RTX Corp)
5. **GLEIF name search is defined but never called** in the main pipeline
6. **Substring matching is too conservative** (7-char minimum blocks "AECOM")

---

## Issue 1: CAGE -> GLEIF -> LEI -> OpenFIGI Path (Tier 1) is Nearly Useless

**Stats:** 399 CAGE codes queried via GLEIF `filter[registered_as]`, only **1** returned an LEI.

**Root Cause:** GLEIF's `registered_as` field is NOT the CAGE code. It's the company registration number (e.g., state incorporation number). The `cage_resolver.py` queries `filter[registered_as]=<CAGE>` — this is a semantic mismatch. CAGE codes are DLA-assigned identifiers with no presence in GLEIF records.

**Fix:** Abandon CAGE->GLEIF direct lookup. Instead:
- Use `sam_entity_client.py` (already built!) to resolve CAGE -> canonical legal name via SAM.gov Entity API
- Then use that canonical name through Tiers 2-4 (EDGAR matching)
- SAM.gov is the **authoritative source** for CAGE codes — it's who issued them

**Impact:** This alone could add 50-100+ resolutions because SAM.gov canonical names are more standardized than the names in contract CSV rows.

---

## Issue 2: EDGAR Map Has Wrong Tickers (Preferred Stock, Warrants)

**Stats:** 259/8,061 entries map to tickers with dashes (preferred stock: BA-PA, warrants, etc.)

**Example:** `BOEING CO -> BA-PA` (a preferred stock class, not the common equity BA).

**Root Cause:** `sec-cik-mapper` iterates `ticker_to_company_name` which includes ALL SEC-registered securities. When multiple tickers share a CIK, whichever is iterated last wins. This means a preferred stock can overwrite the common stock.

**Fix:** When building the EDGAR map, prefer common stock tickers:
```python
# Skip preferred stock, warrants, units, rights
if any(c in ticker for c in ['-', '/', '+']):
    if ename not in edgar_map:  # don't overwrite common with preferred
        continue
```
Or better: use `sec-cik-mapper`'s `cik_to_tickers` (plural) and pick the shortest/simplest ticker per CIK.

**Impact:** Prevents ~259 wrong-ticker matches and fixes Boeing, a top defense contractor.

---

## Issue 3: No Parent Company / Subsidiary Resolution

**Stats:** ~369 non-LLC entities unresolved. Many are subsidiaries:
- `AECOM TECHNICAL SERVICES, INC.` -> parent AECOM (ACM)
- `RAYTHEON BBN TECHNOLOGIES CORP.` -> parent RTX (RTX)
- `DIGITAL MANAGEMENT INC` -> was acquired by Unisys
- `POWER PARAGON INC` -> subsidiary of L3Harris

**Root Cause:** The resolver tries 4 names (legal, contractor, dba, parent) but:
1. The `parent_name` field in SAM.gov CSVs contains "Ultimate Parent Legal Business Name" which IS often the public company — but it's only tried at the same tier level, not prioritized
2. No special handling for known subsidiary patterns

**Fix — Quick Win:** Add a **Tier 2.5** that specifically tries `parent_name` with EDGAR exact match FIRST, before trying fuzzy on the subsidiary name. The parent is much more likely to be the SEC-registered entity.

**Fix — Better:** Build a parent-ticker lookup from the CSV data itself:
```python
# In stage1, collect unique parent_name -> parent_uei mappings
# Many parent names ARE the SEC-registered name (e.g., "AECOM" matches EDGAR directly)
# Resolve parent_name once, apply to all subsidiaries
```

**Impact:** Could resolve 100-200+ additional entities. Government contracts heavily use subsidiary names.

---

## Issue 4: Renamed Companies (Former Names) Never Match

**Example:** Contract says `RAYTHEON COMPANY` but SEC says `RTX CORP` (renamed in 2020).

**Root Cause:** The EDGAR map only has current names. `_validate_candidate()` checks `formerNames` from SEC submissions — but only AFTER a candidate CIK is already found via exact/fuzzy match. If the name doesn't fuzzy-match the current name at all (Raytheon vs RTX = ~25% similarity), it never gets to validation.

**Fix:** Build a **former names index** at init time:
```python
# Download SEC submissions for top N companies (or cache)
# Or simpler: maintain a manual aliases dict for major defense/gov contractors
KNOWN_ALIASES = {
    "RAYTHEON": "RTX",
    "RAYTHEON COMPANY": "RTX",
    "RAYTHEON BBN TECHNOLOGIES": "RTX",
    "UNITED TECHNOLOGIES": "RTX",
    "HARRIS CORPORATION": "LHX",
    "L3 TECHNOLOGIES": "LHX",
    # etc.
}
```
This is a targeted fix. A more scalable approach: download the SEC full-text search company name list which includes former names.

**Impact:** Fixes Raytheon (a HUGE government contractor) and ~20-50 other renamed companies.

---

## Issue 5: `_gleif_name_to_ticker()` is Defined But Never Called

**Location:** `ticker_resolver_v4.py:449-511`

The method exists and does GLEIF company name search -> LEI -> OpenFIGI -> ticker. It has timeout handling, circuit breakers, etc. But it is **never invoked** anywhere in `_resolve()`.

**Root Cause:** Likely an incomplete refactor — the method was written but never wired into the pipeline.

**Fix:** Add a call after CAGE resolution fails and before Tier 2:
```python
# After Tier 1 CAGE path fails, try GLEIF name search
if not r or not r.get("resolved_ticker"):
    for name in names:
        r = self._gleif_name_to_ticker(
            _strip_suffixes(_normalize(name)), name, _normalize(name), "gleif_name"
        )
        if r:
            return r
```

**Caveat:** GLEIF name search is slow (4s timeout per attempt, 2 attempts per name, 4 names = up to 32s worst case). Use it sparingly — maybe only for names that have no EDGAR match at all.

**Impact:** Moderate. GLEIF name search catches some international companies with US listings.

---

## Issue 6: Substring Matching Too Conservative

**Config:** Minimum 7 characters for substring match, plus 50% length ratio.

**Problem:** `AECOM` (5 chars) never matches `AECOM TECHNICAL SERVICES` via substring because `len("AECOM") = 5 < 7`.

**Fix:** Lower minimum to 4-5 characters for the substring match, but add a secondary validation:
```python
if best_len < 5:  # was 7
    return None
# Also require the match to be a complete word boundary
```

**Impact:** Catches short-named public companies (AECOM, SAIC, CACI, etc.) — these are major gov contractors.

---

## Issue 7: Market Cap Validation Kills Valid Matches

**Code:** Every tier calls `_get_market_cap(ticker)` and returns nothing if `mc <= 0`.

**Problem:** `yfinance.fast_info.market_cap` returns 0 or errors for:
- Recently delisted companies (still valid for backtesting!)
- Companies with temporary data issues
- Warrants/preferred classes (see Issue 2)

**Fix:** Make market cap a **filter** not a **gate**. Resolve the ticker, report market cap as 0, let downstream stages decide:
```python
mc = self._get_market_cap(ticker)
# Don't return {} on mc <= 0 — still return the result with mc=0
# Let filter_engine decide if mc=0 is acceptable
```

**Impact:** Prevents dropping valid resolutions that happen to have yfinance data gaps.

---

## Issue 8: No Use of USASpending Bulk CSV Data You Already Have

You have the full USASpending bulk CSV with fields like `recipient_parent_name`, `cage_code`, `recipient_name`, etc. But the resolver treats each entity independently.

**Fix — Build a Company Alias Table from your CSV:**
```python
# From your Stage 1 data, build:
#   cage_code -> set of names used
#   parent_name -> set of subsidiary names
#   UEI -> set of names
# This creates a "government contractor name graph"
# When resolving, if direct name fails, try all known aliases for that entity
```

**Impact:** Huge. The same company appears under different names across contracts. One resolution propagates to all aliases.

---

## Issue 9: Rate Limiting / Speed Issues

**Current bottlenecks:**
- GLEIF API: 10s timeout, no rate limit but slow
- EDGAR throttle: 0.12s per request (good, matches SEC limit)
- yfinance: No rate limit, but slow for mcap lookups
- OpenFIGI: No rate limit, but sequential calls

**Fixes:**
1. **Batch OpenFIGI:** The API accepts up to 100 LEI lookups per POST. Currently doing 1 at a time.
2. **Cache yfinance aggressively:** `.mcap_cache.json` already exists but only has 791 lines. Pre-populate it for all EDGAR map tickers in a batch run.
3. **Skip external APIs for EDGAR-resolved entities:** If exact EDGAR match with CIK, you already have the ticker. Don't call yfinance to "validate" — it's unnecessary.
4. **Parallelize:** Stage 2 resolves entities sequentially. Since most are cache hits or EDGAR lookups (no API), you could parallelize the API-dependent ones.

---

## Issue 10: `_SUFFIX_WORDS` Over-Strips State Abbreviations

```python
_SUFFIX_WORDS includes: "CO", "DE", "MD", "NV", "NY", "VA", "CA", etc.
```

**Problem:** `CO` conflicts with "COMPANY" abbreviation AND the state Colorado. More critically, these state suffixes strip too aggressively:
- `GENERAL DYNAMICS CORP` -> strips `CORP` -> `GENERAL DYNAMICS` (fine)
- `SOME COMPANY DE` -> strips `DE` -> `SOME COMPANY` (fine)
- `NORTH CAROLINA CO` -> strips `CO` -> `NORTH CAROLINA` (bad — it was part of the name)

This rarely causes false matches but occasionally prevents correct ones.

---

## Priority Action Plan

| Priority | Fix | Effort | Expected Impact |
|----------|-----|--------|-----------------|
| **P0** | Fix EDGAR map preferred stock issue (#2) | 30 min | +20-30 correct resolutions |
| **P0** | Wire in `_gleif_name_to_ticker` or remove dead code (#5) | 15 min | Code hygiene + moderate gains |
| **P0** | Lower substring min from 7 to 4 chars (#6) | 5 min | +10-20 (AECOM, SAIC, CACI, etc.) |
| **P1** | Use SAM.gov Entity API for CAGE->canonical name (#1) | 1 hr | +50-100 resolutions |
| **P1** | Add parent-name priority resolution (#3) | 45 min | +100-200 resolutions |
| **P1** | Build known-alias dict for renamed companies (#4) | 30 min | +20-50 (Raytheon, Harris, etc.) |
| **P2** | Don't gate on market cap <= 0 (#7) | 15 min | +10-30 edge cases |
| **P2** | Build alias table from CSV data (#8) | 2 hr | +50-100 resolutions |
| **P3** | Batch OpenFIGI calls (#9) | 1 hr | Speed improvement, not resolution |
| **P3** | Pre-populate mcap cache (#9) | 30 min | Speed improvement |

**Realistic new resolution rate after P0+P1 fixes: 25-35%**
**After all fixes: 30-40%**

The remaining 60-70% are genuinely private companies (LLCs, sole proprietors, nonprofits) that correctly have no ticker.

---

## Appendix: Current Pipeline Flow

```
ContractRecord
  |
  v
Tier 0: Non-public filter (regex + business-type flags + country)
  |  Kills: 54 entities (correct)
  v
Tier 1: CAGE -> GLEIF registered_as -> LEI -> OpenFIGI
  |  Kills: ~398 entities (BROKEN - 0.25% hit rate)
  |  Should be: CAGE -> SAM.gov Entity API -> canonical name -> Tier 2
  v
Tier 2: EDGAR exact match (4 names)
  |  Resolves: 84 entities (working)
  |  Misses: renamed companies, subsidiary names
  v
Tier 3: EDGAR fuzzy match (threshold 70-75)
  |  Resolves: 4 entities (working but low yield)
  v
Tier 4: Substring match (7 char min, 50% ratio)
  |  Resolves: 8 entities (too conservative)
  v
Tier 5: Sole-source tag (no resolution, just flagging)
  |  Tags: 89 entities
  v
Unresolved: 571 entities -> "no_match"
```

---

## Appendix: EDGAR Map Quality

- **Source:** `sec-cik-mapper` (8,061 entries after dedup)
- **Problem entries:** 259 with preferred stock tickers (dashes)
- **Missing former names:** No Raytheon, no United Technologies, no Harris Corp
- **State suffixes:** 535 entries have `/DE/`, `/NV/` etc. — `_normalize()` strips these to empty string via `[^A-Z0-9 ]` regex, which is correct

## Appendix: Cache Health

| Cache File | Entries | Hit Rate | Notes |
|------------|---------|----------|-------|
| `.ticker_cache_v4.json` | 810 | 11.9% resolved | Main resolver cache |
| `.cage_lei_cache.json` | 399 | 0.25% (1 LEI) | GLEIF registered_as lookup — broken |
| `.lei_ticker_cache.json` | 2 | 50% (1 ticker) | Nearly unused |
| `.mcap_cache.json` | ~100 tickers | N/A | yfinance market cap cache |
| `.sam_entity_cache.json` | ~10 | N/A | SAM.gov Entity API — barely used |
| `.edgar_tickers.json` | 8,061 | N/A | EDGAR company->ticker map |
