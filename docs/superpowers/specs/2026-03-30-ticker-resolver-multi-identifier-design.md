# Ticker Resolver: Multi-Identifier Fusion Design
**Date:** 2026-03-30
**Problem:** Current resolution rate ~4.9% (reliant only on SEC EDGAR, ~13K public companies).
**Goal:** Maximize resolution across public/private/delisted/foreign entities using multi-path validation.

---

## 1. Architecture Overview

**Three-tier resolution pipeline** with graceful fallback:

```
INPUT: Awardee name [+ optional CAGE + optional parent name]
  ↓
TIER 1: Native Federal Identifiers
  ├─ CAGE code (if provided) → LEI lookup (GLEIF)
  └─ LEI → OpenFIGI ticker mapping
  ↓
TIER 2: SEC Public Markets (existing, refactored)
  ├─ Exact match (EDGAR map)
  ├─ Substring match (subsidiaries)
  └─ Fuzzy match + SEC validation
  ↓
TIER 3: Alternative Sources (fallback)
  ├─ SEC historical names, parent company mapping
  └─ Multi-source fuzzy validation (3+ sources = high confidence)
  ↓
CONFIDENCE SCORING: Multi-path consensus → confidence level
  ↓
OUTPUT: {ticker, cik, confidence, evidence_type, audit_trail}
```

**Key principle:** Each identifier path is independent; results validated via cross-check. If CAGE→LEI→OpenFIGI and SEC EDGAR both return same ticker, confidence = "very_high".

---

## 2. Components & Responsibilities

### 2.1 New: `CageResolver`
**Purpose:** Resolve CAGE code (5-char) → LEI via government contractor databases.

**Data source:** CAGE codes embedded in USASpending CSV (if available) or SAM.gov API (live).

**Responsibility:**
- Parse CAGE from contract data
- Query GLEIF LEI API (free) with CAGE as hint
- Validate LEI format (20-char alphanumeric, ISO 17442)
- Return LEI + confidence score

**API:** `resolve_cage(cage_code: str) → dict{lei, confidence, source}`

---

### 2.2 New: `LeiResolver`
**Purpose:** Map Legal Entity Identifier (LEI) → ticker via OpenFIGI.

**Data source:** OpenFIGI API (free), GLEIF API (free).

**Responsibility:**
- Query OpenFIGI: LEI → FIGI → ticker mapping
- Cross-validate via GLEIF entity lookup (legal name check)
- Handle missing tickers (private/delisted entities)
- Return ticker + confidence score

**API:** `resolve_lei(lei: str) → dict{ticker, cik, confidence, entity_type, source}`

---

### 2.3 Refactored: `TickerResolverV3`
**Purpose:** Orchestrate multi-path resolution; maintain backward compatibility for USASpending training.

**Changes vs. V2:**
- Add `cage_code` parameter to `resolve()` (optional, None for USASpending; provided for SAM.gov)
- Tier 1: Call `CageResolver` if CAGE provided
- Tier 2: Use existing SEC logic (exact, substring, fuzzy)
- Tier 3: For unresolved, attempt LEI lookup as fallback (via parent company or fuzzy-matched company registration)
- New method: `_multi_path_consensus()` — if ≥2 independent paths return same ticker, boost confidence to "very_high"

**API:**
```python
resolve(awardee_name, parent_name="", cage_code=None) → dict{
  resolved_ticker, resolved_cik, confidence, evidence_type,
  audit_trail: [{path, source, result, score}], market_cap
}
```

---

### 2.4 New: `ApiCache` (shared utility)
**Purpose:** Cache external API responses (LEI, OpenFIGI, GLEIF) to minimize rate-limit hits.

**Responsibility:**
- Persistent disk cache (JSON): `{query_key → result, ttl}`
- TTL: 30 days for LEI/entity data, 7 days for tickers (volatile)
- Thread-safe reads/writes
- Auto-expire stale entries on load

**API:** `cache.get(key)`, `cache.set(key, value, ttl_days)`, `cache.clear_expired()`

---

## 3. Data Flow

### 3.1 Training Pipeline (USASpending CSV)
```
USASpending CSV row
  ├─ recipient_name, recipient_parent_name, contract_value, award_date
  └─ (NO CAGE code in bulk export)
       ↓
TickerResolverV3.resolve(awardee_name, parent_name=parent, cage_code=None)
       ├─ Tier 1 skipped (no CAGE)
       ├─ Tier 2: SEC exact/substring/fuzzy matching
       └─ Tier 3: If unresolved, attempt fuzzy LEI lookup (optional enrichment)
            ↓
Output: stage2_with_tickers.csv {original_name, ticker, confidence, evidence_type, audit_trail}
```

### 3.2 Live Trading Pipeline (SAM.gov API)
```
SAM.gov API response
  ├─ recipient_name, recipient_parent_name, cage_code, contract_value, award_date
       ↓
TickerResolverV3.resolve(awardee_name, parent_name=parent, cage_code=cage)
       ├─ Tier 1: CAGE → LEI (via GLEIF)
       ├─ LEI → ticker (via OpenFIGI)
       ├─ Tier 2: SEC fallback if Tier 1 fails
       └─ Tier 3: Fuzzy consolidation if both paths found matches
            ↓
Output: {ticker, confidence: "very_high"|"high"|"medium"|"low", audit_trail}
```

**Key difference:** Live trading has CAGE codes, enabling higher-confidence resolution.

---

## 4. Confidence Scoring & Audit Trail

### Confidence Levels
- **very_high:** Multiple paths (CAGE+LEI+SEC or SEC multi-source) point to **same ticker**
- **high:** Single path with SEC name validation OR CAGE→LEI with entity name match
- **medium:** Fuzzy match (90+) with SEC validation OR substring match with mcap verification
- **low:** Fuzzy match (85-89) OR sole-source path without cross-validation
- **none:** No match or non-public entity

### Audit Trail
Each resolution includes `audit_trail: [{path, source, confidence_component, result}]`:
```json
{
  "audit_trail": [
    {"path": "cage_to_lei", "source": "GLEIF", "confidence": 0.95, "result": "LEI:12345..."},
    {"path": "lei_to_ticker", "source": "OpenFIGI", "confidence": 0.90, "result": "ACME:ACME"},
    {"path": "sec_fuzzy", "source": "EDGAR", "confidence": 0.85, "result": "ACME:ACME"},
    {"consensus": "both_paths_match", "final_confidence": "very_high"}
  ]
}
```

---

## 5. Error Handling & Graceful Degradation

| Scenario | Behavior |
|----------|----------|
| CAGE invalid/empty | Skip Tier 1, proceed to Tier 2 (SEC) |
| LEI API unavailable | Cache fallback; if cache empty, skip to Tier 2 |
| OpenFIGI timeout | Retry once with 2s backoff; fallback to Tier 2 |
| SEC rate-limited | Use existing throttle; queue for batch retry |
| No match across all tiers | Mark as `unresolved` with evidence; cache for future enrichment |
| Non-public entity detected | Early exit (pattern matching unchanged from V2) |

**Result:** Even if one or more external APIs fail, resolution continues via available paths.

---

## 6. Implementation Scope

### Files to Create
- `cage_resolver.py` — CAGE → LEI resolution
- `lei_resolver.py` — LEI → ticker resolution
- `api_cache.py` — Shared cache utility

### Files to Modify
- `ticker_resolver.py` → `ticker_resolver_v3.py` (rename; add multi-path orchestration)
- `build_training_set.py` — Update Stage 2 to use V3 resolver
- `config.py` — Add new API endpoints, cache TTLs, GLEIF/OpenFIGI keys (if needed)

### External Dependencies
- `requests` (already in use) — HTTP calls to GLEIF/OpenFIGI
- `rapidfuzz` (already in use) — Fuzzy matching for LEI entity name validation
- `sec-cik-mapper` (already in use) — SEC data
- `yfinance` (already in use) — Market cap validation

**No new dependencies required.**

---

## 7. Testing Strategy

### Unit Tests
- `test_cage_resolver.py` — CAGE validation, GLEIF mock responses
- `test_lei_resolver.py` — LEI parsing, OpenFIGI mapping
- `test_api_cache.py` — Cache hit/miss, TTL expiry, thread safety
- `test_ticker_resolver_v3.py` — Multi-path orchestration, consensus logic

### Integration Tests
- End-to-end: USASpending CSV → resolved tickers (should improve from 4.9%)
- Live SAM.gov mock: CAGE present → higher resolution rate
- Cross-validation: Same ticker from multiple paths

### Regression Tests
- Existing V2 tests (non-CAGE path) should still pass with V3

---

## 8. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Overall resolution rate | 4.9% | 25%+ (conservative; 40%+ possible) |
| Confidence distribution | N/A | >50% "very_high" + "high" |
| API cache hit rate | N/A | >80% (after warmup) |
| Resolution latency | <100ms (SEC-only) | <200ms (multi-path with cache) |

---

## 9. Phased Rollout

**Phase 1:** Implement `CageResolver`, `LeiResolver`, `ApiCache` (isolated, tested)
**Phase 2:** Refactor `TickerResolverV2` → `V3` with multi-path orchestration
**Phase 3:** Update `build_training_set.py` to use V3; run full training pipeline
**Phase 4:** Measure resolution rate improvements; tune thresholds
**Phase 5:** Integrate with live SAM.gov trading pipeline (add CAGE parameter)

---

## 10. Open Questions / Assumptions

1. **USASpending CAGE availability:** Assumed NOT in bulk CSV export; can be added if available.
2. **LEI coverage:** Assumes government contractors have LEIs (may vary; fallback to SEC).
3. **OpenFIGI rate limits:** Free tier allows reasonable daily volume; monitored via cache.
4. **Entity name fuzzy matching:** Uses rapidfuzz (existing); may need tuning for LEI entities.
5. **Parent company mapping:** Existing logic maintained; enriched via SEC historical names.

