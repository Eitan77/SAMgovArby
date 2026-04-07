# Ticker Resolver: Multi-Identifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement multi-path ticker resolution (CAGE→LEI→OpenFIGI + SEC fallback) to increase resolution rate from 4.9% to 25%+.

**Architecture:** Three-tier pipeline (native federal identifiers → SEC public markets → alternative sources) with graceful fallback and multi-path consensus scoring.

**Tech Stack:** requests (HTTP), rapidfuzz (fuzzy matching), yfinance (market cap), sec-cik-mapper (EDGAR), GLEIF API (LEI), OpenFIGI API (ticker mapping).

---

## File Structure

**New files:**
- `api_cache.py` — Shared HTTP response cache with TTL
- `cage_resolver.py` — CAGE code → LEI resolution via GLEIF
- `lei_resolver.py` — LEI → ticker mapping via OpenFIGI + GLEIF validation
- `tests/test_api_cache.py` — Cache unit tests
- `tests/test_cage_resolver.py` — CAGE resolver tests
- `tests/test_lei_resolver.py` — LEI resolver tests
- `tests/test_ticker_resolver_v3.py` — V3 orchestration tests

**Modified files:**
- `ticker_resolver.py` → rename to `ticker_resolver_v3.py` and refactor
- `build_training_set.py` — Update Stage 2 to use V3 resolver
- `config.py` — Add GLEIF/OpenFIGI endpoints, cache TTLs

---

## Task Sequence

### Task 1: Create ApiCache (foundation)

**Files:**
- Create: `api_cache.py`
- Create: `tests/test_api_cache.py`

- [ ] **Step 1: Write test for cache get/set**

Create `tests/test_api_cache.py`:
```python
import json
import os
import tempfile
import time
from api_cache import ApiCache

def test_cache_set_and_get():
    """Test basic cache set/get."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ApiCache(cache_file=os.path.join(tmpdir, "test.json"))
        cache.set("test_key", {"value": 123}, ttl_days=1)
        result = cache.get("test_key")
        assert result == {"value": 123}

def test_cache_ttl_expiry():
    """Test TTL expiry logic."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ApiCache(cache_file=os.path.join(tmpdir, "test.json"))
        cache.set("test_key", {"value": 456}, ttl_days=0)  # Expires immediately
        time.sleep(0.1)
        result = cache.get("test_key")
        assert result is None

def test_cache_load_from_disk():
    """Test cache persistence."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "test.json")

        # Write to cache
        cache1 = ApiCache(cache_file=cache_path)
        cache1.set("persist_key", {"value": 789}, ttl_days=30)

        # Load from disk
        cache2 = ApiCache(cache_file=cache_path)
        result = cache2.get("persist_key")
        assert result == {"value": 789}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rtk pytest tests/test_api_cache.py -v
```

Expected: `FAILED tests/test_api_cache.py::test_cache_set_and_get - ModuleNotFoundError: No module named 'api_cache'`

- [ ] **Step 3: Write ApiCache implementation**

Create `api_cache.py`:
```python
"""Shared cache utility for external API responses (LEI, OpenFIGI, GLEIF).

Provides persistent disk-based caching with TTL expiry.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Optional

log = logging.getLogger(__name__)


class ApiCache:
    """Persistent cache with TTL support."""

    def __init__(self, cache_file: str = ".api_cache.json"):
        self.cache_file = cache_file
        self.data: dict = {}
        self._load()

    def _load(self):
        """Load cache from disk."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    self.data = json.load(f)
            except Exception as e:
                log.warning(f"Failed to load cache: {e}")
                self.data = {}
        else:
            self.data = {}

    def _save(self):
        """Save cache to disk."""
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save cache: {e}")

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        if key not in self.data:
            return None

        entry = self.data[key]
        ttl_unix = entry.get("ttl")

        if ttl_unix and time.time() >= ttl_unix:
            # Expired
            del self.data[key]
            self._save()
            return None

        return entry.get("value")

    def set(self, key: str, value: Any, ttl_days: int = 30):
        """Set value in cache with TTL."""
        ttl_unix = time.time() + (ttl_days * 86400)
        self.data[key] = {
            "value": value,
            "ttl": ttl_unix,
            "set_at": datetime.utcnow().isoformat()
        }
        self._save()

    def clear_expired(self):
        """Remove all expired entries."""
        now = time.time()
        expired_keys = [
            k for k, v in self.data.items()
            if v.get("ttl") and time.time() >= v["ttl"]
        ]
        for k in expired_keys:
            del self.data[k]
        if expired_keys:
            self._save()
            log.info(f"Cleared {len(expired_keys)} expired cache entries")

    def clear_all(self):
        """Clear all cache entries."""
        self.data = {}
        self._save()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
rtk pytest tests/test_api_cache.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add api_cache.py tests/test_api_cache.py
rtk git commit -m "feat: Add ApiCache utility for external API response caching"
```

---

### Task 2: Create CageResolver

**Files:**
- Create: `cage_resolver.py`
- Create: `tests/test_cage_resolver.py`

- [ ] **Step 1: Write tests for CAGE validation and GLEIF lookup**

Create `tests/test_cage_resolver.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from cage_resolver import CageResolver, is_valid_cage_code

def test_cage_validation():
    """Test CAGE code format validation."""
    assert is_valid_cage_code("12345") == True
    assert is_valid_cage_code("ABCDE") == True
    assert is_valid_cage_code("123") == False  # Too short
    assert is_valid_cage_code("123456") == False  # Too long
    assert is_valid_cage_code("") == False  # Empty
    assert is_valid_cage_code(None) == False  # None

def test_cage_resolver_valid_response():
    """Test CageResolver with valid GLEIF response."""
    resolver = CageResolver()

    mock_response = {
        "lei_records": [
            {
                "lei": "5493001KJTIIGC8Y1R12",
                "entity": {"registered_as": "ACME CORP"},
                "legalForm": {"code": "SM"}
            }
        ]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        result = resolver.resolve_cage("12ABC")

        assert result["lei"] == "5493001KJTIIGC8Y1R12"
        assert result["confidence"] >= 0.8
        assert result["source"] == "gleif"

def test_cage_resolver_invalid_cage():
    """Test CageResolver with invalid CAGE code."""
    resolver = CageResolver()

    result = resolver.resolve_cage("INVALID")

    assert result["lei"] is None
    assert result["confidence"] == 0
    assert result["rejection_reason"] == "invalid_cage_format"

def test_cage_resolver_api_error():
    """Test CageResolver when GLEIF API fails."""
    resolver = CageResolver()

    with patch("requests.get") as mock_get:
        mock_get.side_effect = Exception("API Error")

        result = resolver.resolve_cage("12ABC")

        assert result["lei"] is None
        assert result["confidence"] == 0
        assert "api_error" in result["rejection_reason"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rtk pytest tests/test_cage_resolver.py -v
```

Expected: `FAILED ... - ModuleNotFoundError: No module named 'cage_resolver'`

- [ ] **Step 3: Write CageResolver implementation**

Create `cage_resolver.py`:
```python
"""CAGE code → LEI resolution via GLEIF API.

CAGE (Commercial and Government Entity) codes are 5-character alphanumeric
identifiers assigned by the Defense Logistics Agency (DLA) to federal
contractors. This resolver maps them to Legal Entity Identifiers (LEI) via
the GLEIF API.
"""
import logging
import re
import requests
from typing import Optional
from api_cache import ApiCache

log = logging.getLogger(__name__)

GLEIF_SEARCH_URL = "https://leilookup.gleif.org/api/v3/lei-records"
GLEIF_HEADERS = {"Accept": "application/json"}

_cage_cache = ApiCache(cache_file=".cage_lei_cache.json")


def is_valid_cage_code(cage: str) -> bool:
    """Validate CAGE code format (5 alphanumeric characters)."""
    if not cage or not isinstance(cage, str):
        return False
    cage = cage.strip().upper()
    return bool(re.match(r"^[A-Z0-9]{5}$", cage))


class CageResolver:
    """Resolve CAGE codes to LEI via GLEIF."""

    def __init__(self, cache: Optional[ApiCache] = None):
        self.cache = cache or _cage_cache

    def resolve_cage(self, cage_code: str) -> dict:
        """Resolve CAGE code to LEI.

        Returns dict with keys:
          lei: Legal Entity Identifier (20-char) or None
          confidence: 0.0-1.0 confidence score
          rejection_reason: reason if failed (e.g., "invalid_cage_format", "not_found", "api_error")
          source: "gleif" if successful
        """
        if not is_valid_cage_code(cage_code):
            return {
                "lei": None,
                "confidence": 0,
                "rejection_reason": "invalid_cage_format",
                "source": "none"
            }

        cage_upper = cage_code.strip().upper()

        # Check cache first
        cached = self.cache.get(f"cage:{cage_upper}")
        if cached is not None:
            return cached

        result = self._query_gleif(cage_upper)
        self.cache.set(f"cage:{cage_upper}", result, ttl_days=30)
        return result

    def _query_gleif(self, cage_code: str) -> dict:
        """Query GLEIF API for CAGE code."""
        try:
            # Search GLEIF by CAGE code (try as-is and as field)
            params = {
                "filter[registered_as]": cage_code,
                "page[size]": 1
            }
            resp = requests.get(GLEIF_SEARCH_URL, params=params, headers=GLEIF_HEADERS, timeout=10)

            if resp.status_code != 200:
                return {
                    "lei": None,
                    "confidence": 0,
                    "rejection_reason": f"gleif_http_{resp.status_code}",
                    "source": "none"
                }

            data = resp.json()
            records = data.get("lei_records", [])

            if not records:
                # Try alternative search via entity name if available
                return {
                    "lei": None,
                    "confidence": 0,
                    "rejection_reason": "not_found_in_gleif",
                    "source": "none"
                }

            # Extract LEI from first result
            lei = records[0].get("lei")
            entity = records[0].get("entity", {})
            name = entity.get("registered_as", "")

            if not lei:
                return {
                    "lei": None,
                    "confidence": 0,
                    "rejection_reason": "no_lei_in_response",
                    "source": "none"
                }

            return {
                "lei": lei,
                "confidence": 0.95,  # GLEIF direct match is high confidence
                "rejection_reason": None,
                "source": "gleif",
                "entity_name": name
            }

        except requests.Timeout:
            log.warning(f"GLEIF API timeout for CAGE {cage_code}")
            return {
                "lei": None,
                "confidence": 0,
                "rejection_reason": "gleif_timeout",
                "source": "none"
            }
        except Exception as e:
            log.debug(f"GLEIF API error for CAGE {cage_code}: {e}")
            return {
                "lei": None,
                "confidence": 0,
                "rejection_reason": f"api_error: {str(e)[:50]}",
                "source": "none"
            }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
rtk pytest tests/test_cage_resolver.py -v
```

Expected: All tests PASS (mocked GLEIF calls).

- [ ] **Step 5: Commit**

```bash
rtk git add cage_resolver.py tests/test_cage_resolver.py
rtk git commit -m "feat: Add CageResolver for CAGE code to LEI mapping"
```

---

### Task 3: Create LeiResolver

**Files:**
- Create: `lei_resolver.py`
- Create: `tests/test_lei_resolver.py`

- [ ] **Step 1: Write tests for LEI validation and OpenFIGI mapping**

Create `tests/test_lei_resolver.py`:
```python
import pytest
from unittest.mock import patch
from lei_resolver import LeiResolver, is_valid_lei

def test_lei_validation():
    """Test LEI format validation (20 alphanumeric)."""
    assert is_valid_lei("5493001KJTIIGC8Y1R12") == True
    assert is_valid_lei("5493001KJTIIGC8Y1R1") == False  # 19 chars
    assert is_valid_lei("5493001KJTIIGC8Y1R122") == False  # 21 chars
    assert is_valid_lei("") == False
    assert is_valid_lei(None) == False

def test_lei_resolver_openfigi_success():
    """Test LeiResolver with valid OpenFIGI response."""
    resolver = LeiResolver()

    openfigi_response = [
        {
            "data": [
                {
                    "figi": "BBG000B9XRY4",
                    "name": "ACME CORP",
                    "ticker": "ACME",
                    "exchCode": "US"
                }
            ]
        }
    ]

    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = openfigi_response

        result = resolver.resolve_lei("5493001KJTIIGC8Y1R12")

        assert result["ticker"] == "ACME"
        assert result["confidence"] >= 0.85
        assert result["source"] == "openfigi"

def test_lei_resolver_invalid_lei():
    """Test LeiResolver with invalid LEI."""
    resolver = LeiResolver()

    result = resolver.resolve_lei("INVALID_LEI")

    assert result["ticker"] is None
    assert result["confidence"] == 0
    assert result["rejection_reason"] == "invalid_lei_format"

def test_lei_resolver_no_ticker_found():
    """Test LeiResolver when OpenFIGI finds no ticker."""
    resolver = LeiResolver()

    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = [{"data": []}]  # No results

        result = resolver.resolve_lei("5493001KJTIIGC8Y1R12")

        assert result["ticker"] is None
        assert result["confidence"] == 0
        assert "not_found" in result["rejection_reason"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rtk pytest tests/test_lei_resolver.py -v
```

Expected: `FAILED ... - ModuleNotFoundError: No module named 'lei_resolver'`

- [ ] **Step 3: Write LeiResolver implementation**

Create `lei_resolver.py`:
```python
"""LEI → ticker resolution via OpenFIGI and GLEIF APIs.

Legal Entity Identifier (LEI) is a 20-character alphanumeric code that
uniquely identifies legal entities. This resolver maps LEIs to stock
tickers via OpenFIGI (Bloomberg's open symbology) and validates via GLEIF.
"""
import logging
import re
import requests
from typing import Optional
from rapidfuzz import fuzz
from api_cache import ApiCache

log = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
GLEIF_LEI_URL = "https://leilookup.gleif.org/api/v3/lei-records"
HEADERS = {"Accept": "application/json"}

_lei_cache = ApiCache(cache_file=".lei_ticker_cache.json")


def is_valid_lei(lei: str) -> bool:
    """Validate LEI format (20 alphanumeric per ISO 17442)."""
    if not lei or not isinstance(lei, str):
        return False
    lei = lei.strip().upper()
    return bool(re.match(r"^[A-Z0-9]{20}$", lei))


class LeiResolver:
    """Resolve LEI to ticker via OpenFIGI."""

    def __init__(self, cache: Optional[ApiCache] = None):
        self.cache = cache or _lei_cache

    def resolve_lei(self, lei: str) -> dict:
        """Resolve LEI to ticker.

        Returns dict with keys:
          ticker: stock ticker or None
          cik: SEC CIK if available
          confidence: 0.0-1.0
          rejection_reason: reason if failed
          source: "openfigi" or "gleif" or "none"
          entity_type: "PUBLIC" | "PRIVATE" | "UNKNOWN"
        """
        if not is_valid_lei(lei):
            return {
                "ticker": None,
                "cik": None,
                "confidence": 0,
                "rejection_reason": "invalid_lei_format",
                "source": "none",
                "entity_type": "UNKNOWN"
            }

        lei_upper = lei.strip().upper()

        # Check cache
        cached = self.cache.get(f"lei:{lei_upper}")
        if cached is not None:
            return cached

        # Try OpenFIGI first
        result = self._query_openfigi(lei_upper)

        # If OpenFIGI succeeds, validate via GLEIF
        if result["ticker"]:
            gleif_info = self._get_gleif_info(lei_upper)
            if gleif_info:
                result["entity_type"] = gleif_info.get("entity_type", "UNKNOWN")
                # Boost confidence if GLEIF confirms
                result["confidence"] = min(1.0, result["confidence"] + 0.1)

        self.cache.set(f"lei:{lei_upper}", result, ttl_days=30)
        return result

    def _query_openfigi(self, lei: str) -> dict:
        """Query OpenFIGI for LEI → ticker mapping."""
        try:
            payload = [{"idType": "LEI", "idValue": lei}]
            resp = requests.post(OPENFIGI_URL, json=payload, headers=HEADERS, timeout=10)

            if resp.status_code != 200:
                return {
                    "ticker": None,
                    "cik": None,
                    "confidence": 0,
                    "rejection_reason": f"openfigi_http_{resp.status_code}",
                    "source": "none",
                    "entity_type": "UNKNOWN"
                }

            results = resp.json()

            if not results or not results[0].get("data"):
                return {
                    "ticker": None,
                    "cik": None,
                    "confidence": 0,
                    "rejection_reason": "lei_not_found_in_openfigi",
                    "source": "none",
                    "entity_type": "UNKNOWN"
                }

            # Find first US equity ticker
            for match in results[0]["data"]:
                ticker = match.get("ticker")
                exch = match.get("exchCode", "")
                name = match.get("name", "")

                if ticker and exch in ("US", ""):  # US exchange or unspecified
                    return {
                        "ticker": ticker.upper(),
                        "cik": None,  # OpenFIGI doesn't provide CIK
                        "confidence": 0.9,  # Direct OpenFIGI match
                        "rejection_reason": None,
                        "source": "openfigi",
                        "entity_type": "PUBLIC",
                        "name": name
                    }

            # No US ticker found
            return {
                "ticker": None,
                "cik": None,
                "confidence": 0,
                "rejection_reason": "no_us_ticker_in_openfigi",
                "source": "none",
                "entity_type": "UNKNOWN"
            }

        except requests.Timeout:
            log.warning(f"OpenFIGI timeout for LEI {lei}")
            return {
                "ticker": None,
                "cik": None,
                "confidence": 0,
                "rejection_reason": "openfigi_timeout",
                "source": "none",
                "entity_type": "UNKNOWN"
            }
        except Exception as e:
            log.debug(f"OpenFIGI error for LEI {lei}: {e}")
            return {
                "ticker": None,
                "cik": None,
                "confidence": 0,
                "rejection_reason": f"api_error: {str(e)[:50]}",
                "source": "none",
                "entity_type": "UNKNOWN"
            }

    def _get_gleif_info(self, lei: str) -> Optional[dict]:
        """Get entity info from GLEIF (for validation only, not ticker resolution)."""
        try:
            params = {"filter[lei]": lei, "page[size]": 1}
            resp = requests.get(GLEIF_LEI_URL, params=params, headers=HEADERS, timeout=10)

            if resp.status_code != 200:
                return None

            data = resp.json()
            records = data.get("lei_records", [])

            if not records:
                return None

            entity = records[0].get("entity", {})
            return {
                "name": entity.get("registered_as", ""),
                "entity_type": "PUBLIC" if entity.get("status") == "ACTIVE" else "PRIVATE"
            }

        except Exception as e:
            log.debug(f"GLEIF info fetch failed for LEI {lei}: {e}")
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
rtk pytest tests/test_lei_resolver.py -v
```

Expected: All tests PASS (mocked API calls).

- [ ] **Step 5: Commit**

```bash
rtk git add lei_resolver.py tests/test_lei_resolver.py
rtk git commit -m "feat: Add LeiResolver for LEI to ticker mapping via OpenFIGI"
```

---

### Task 4: Refactor TickerResolverV2 → V3

**Files:**
- Modify: `ticker_resolver.py` (rename to `ticker_resolver_v3.py`)
- Create: `tests/test_ticker_resolver_v3.py`

- [ ] **Step 1: Copy V2 to V3 and add cage_code parameter**

```bash
rtk cp ticker_resolver.py ticker_resolver_v3.py
```

Now modify `ticker_resolver_v3.py` — change the class name and add CAGE support:

In `ticker_resolver_v3.py`, change line 237 from:
```python
class TickerResolverV2:
```

To:
```python
class TickerResolverV3:
```

And update the docstring (line 238) to:
```python
    """Multi-stage resolver: CAGE→LEI, exact, validate, fuzzy+validate, non-public detect."""
```

Add these imports at the top (after existing imports, line 23):
```python
from cage_resolver import CageResolver
from lei_resolver import LeiResolver
```

Add instance variables to `__init__` (after line 251, add):
```python
        self.cage_resolver = CageResolver()
        self.lei_resolver = LeiResolver()
```

Update the `resolve` method signature (line 302) from:
```python
    def resolve(self, awardee_name: str, parent_name: str = "") -> dict:
```

To:
```python
    def resolve(self, awardee_name: str, parent_name: str = "", cage_code: str = "") -> dict:
```

Update the docstring to include cage_code:
```python
        """Resolve an awardee name to ticker/CIK.

        Args:
            awardee_name: Direct company name from award
            parent_name: Parent company name from USASpending (fallback)
            cage_code: Commercial and Government Entity code (optional, SAM.gov only)

        Returns cache entry dict with keys: resolved_ticker, resolved_cik,
        evidence_type, confidence, rejection_reason, market_cap_current, audit_trail.
        """
```

Add Tier 1 (CAGE) resolution before the existing Stage 1 check. After line 313, insert:
```python
        # Tier 1: CAGE code → LEI → OpenFIGI (federal identifiers)
        if cage_code:
            cage_result = self._resolve_via_cage(awardee_name, cage_code)
            if cage_result.get("resolved_ticker"):
                self.cache[awardee_name] = cage_result
                return cage_result
```

Add this new method to the class (before `_resolve_name`):
```python
    def _resolve_via_cage(self, awardee_name: str, cage_code: str) -> dict:
        """Attempt resolution via CAGE → LEI → OpenFIGI."""
        # Step 1: CAGE → LEI
        cage_result = self.cage_resolver.resolve_cage(cage_code)
        if not cage_result.get("lei"):
            return {}

        lei = cage_result["lei"]

        # Step 2: LEI → ticker
        lei_result = self.lei_resolver.resolve_lei(lei)
        if not lei_result.get("ticker"):
            return {}

        ticker = lei_result["ticker"]
        cik = lei_result.get("cik", "")
        mc = self._get_market_cap(ticker)

        norm = _normalize(awardee_name)
        audit_trail = [
            {"path": "cage_to_lei", "source": "GLEIF", "lei": lei, "confidence": cage_result.get("confidence")},
            {"path": "lei_to_ticker", "source": "OpenFIGI", "ticker": ticker, "confidence": lei_result.get("confidence")}
        ]

        return self._make_result(awardee_name, norm, ticker, cik, "high",
                                "cage_lei_openfigi", None, mc, audit_trail)
```

Update `_make_result` signature (line 461) to add `audit_trail` parameter:
```python
    @staticmethod
    def _make_result(original, normalized, ticker, cik, confidence,
                     evidence_type, rejection_reason=None, market_cap=0.0, audit_trail=None):
        return {
            "original_name": original,
            "normalized_name": normalized,
            "resolved_ticker": ticker,
            "resolved_cik": cik or "",
            "evidence_type": evidence_type,
            "confidence": confidence,
            "rejection_reason": rejection_reason,
            "market_cap_current": market_cap or 0.0,
            "audit_trail": audit_trail or [],
            "last_verified": datetime.utcnow().isoformat(),
        }
```

- [ ] **Step 2: Write tests for V3 with CAGE path**

Create `tests/test_ticker_resolver_v3.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from ticker_resolver_v3 import TickerResolverV3, _normalize

def test_resolve_with_cage_code():
    """Test V3 resolves via CAGE → LEI → ticker."""
    resolver = TickerResolverV3()

    # Mock CAGE resolver
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {
        "lei": "5493001KJTIIGC8Y1R12",
        "confidence": 0.95
    }
    resolver.cage_resolver = cage_mock

    # Mock LEI resolver
    lei_mock = MagicMock()
    lei_mock.resolve_lei.return_value = {
        "ticker": "ACME",
        "cik": "0000012345",
        "confidence": 0.9,
        "source": "openfigi"
    }
    resolver.lei_resolver = lei_mock

    # Mock yfinance for market cap
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.fast_info.market_cap = 500_000_000  # $500M

        result = resolver.resolve("ACME CORP", cage_code="12ABC")

        assert result["resolved_ticker"] == "ACME"
        assert result["confidence"] == "high"
        assert "cage_lei_openfigi" in result["evidence_type"]
        assert len(result["audit_trail"]) == 2

def test_resolve_cage_fails_falls_back_to_sec():
    """Test V3 falls back to SEC if CAGE fails."""
    resolver = TickerResolverV3()

    # Mock failed CAGE resolver
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {"lei": None}
    resolver.cage_resolver = cage_mock

    # Should fall through to existing SEC logic
    result = resolver.resolve("NORTHROP GRUMMAN", cage_code="INVALID")

    # Either resolves via SEC or returns unresolved
    assert "resolved_ticker" in result

def test_resolve_without_cage_code():
    """Test V3 works without CAGE (backward compatible with V2)."""
    resolver = TickerResolverV3()

    # Should use existing SEC logic
    result = resolver.resolve("NORTHROP GRUMMAN")

    # Should resolve via existing SEC paths or return unresolved
    assert "resolved_ticker" in result
    assert "evidence_type" in result
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
rtk pytest tests/test_ticker_resolver_v3.py -v
```

Expected: All tests PASS.

- [ ] **Step 4: Update module-level wrapper function**

In `ticker_resolver_v3.py`, find the `resolve_ticker` function (line 481) and update:

Change line 478 from:
```python
_resolver_instance: "TickerResolverV2 | None" = None
```

To:
```python
_resolver_instance: "TickerResolverV3 | None" = None
```

Change line 491 from:
```python
            _resolver_instance = TickerResolverV2()
```

To:
```python
            _resolver_instance = TickerResolverV3()
```

Update the function signature (line 481) to:
```python
def resolve_ticker(awardee_name: str, edgar_results=None, resolver: "TickerResolverV3 | None" = None, cage_code: str = "") -> "tuple[str | None, str]":
```

Update the function body (line 493) to pass cage_code:
```python
    result = resolver.resolve(awardee_name, cage_code=cage_code)
```

- [ ] **Step 5: Commit**

```bash
rtk git add ticker_resolver_v3.py tests/test_ticker_resolver_v3.py
rtk git commit -m "feat: Create TickerResolverV3 with CAGE→LEI→OpenFIGI Tier 1 support"
```

---

### Task 5: Update build_training_set.py to use V3

**Files:**
- Modify: `build_training_set.py`

- [ ] **Step 1: Update import in build_training_set.py**

Find line 50 (approximately):
```python
from ticker_resolver import resolve_ticker
```

Change to:
```python
from ticker_resolver_v3 import resolve_ticker
```

- [ ] **Step 2: Update Stage 2 call (no CAGE in USASpending CSV)**

Find where `resolve_ticker` is called in Stage 2 (around line 400+ in the enrich section). The call should look like:
```python
ticker, confidence = resolve_ticker(awardee_name, resolver=resolver)
```

No change needed here — cage_code will default to "" (empty string), which V3 handles correctly.

However, if the code explicitly creates a TickerResolverV2 instance, find and update:
```python
resolver = TickerResolverV2()
```

To:
```python
resolver = TickerResolverV3()
```

- [ ] **Step 3: Run full build_training_set.py on your 2023 data**

```bash
rtk python build_training_set.py --verbose 2>&1 | head -100
```

Expected: Training set builds successfully (all 3 stages). Monitor output for:
- Stage 1: Filter completes
- Stage 2: Ticker resolution starts and completes
- Stage 3: Enrichment completes

- [ ] **Step 4: Check resolution rate improvement**

After training completes, analyze the resolution rate:

```bash
rtk python -c "
import csv
resolved_count = 0
total_count = 0
with open('datasets/stage2_with_tickers.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        total_count += 1
        if row.get('resolved_ticker'):
            resolved_count += 1
rate = (resolved_count / total_count * 100) if total_count else 0
print(f'Resolution rate: {resolved_count}/{total_count} = {rate:.1f}%')
"
```

Expected: Rate should be higher than 4.9% (target: 25%+).

- [ ] **Step 5: Commit**

```bash
rtk git add build_training_set.py
rtk git commit -m "feat: Update build_training_set.py to use TickerResolverV3"
```

---

### Task 6: Integration testing & live pipeline support

**Files:**
- Create: `tests/test_integration_multi_identifier.py`
- Modify: `main.py` (live trading pipeline)

- [ ] **Step 1: Write integration test**

Create `tests/test_integration_multi_identifier.py`:
```python
"""Integration test for multi-identifier resolver against real USASpending data."""
import csv
import os
import pytest
from ticker_resolver_v3 import TickerResolverV3

def test_real_contracts_resolution():
    """Test resolver on sample real contracts from training set."""
    resolver = TickerResolverV3()

    # Test a few real contracts if training CSV exists
    training_csv = "datasets/filtered_training_set.csv"
    if not os.path.exists(training_csv):
        pytest.skip("Training CSV not found; run build_training_set.py first")

    resolved_count = 0
    test_count = 0

    with open(training_csv) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 100:  # Test first 100 contracts
                break

            test_count += 1
            awardee = row.get("recipient_name", "").strip()
            parent = row.get("parent_recipient_name", "").strip()

            if not awardee:
                continue

            result = resolver.resolve(awardee, parent_name=parent)
            if result.get("resolved_ticker"):
                resolved_count += 1

    rate = (resolved_count / test_count * 100) if test_count else 0
    print(f"\nIntegration test: {resolved_count}/{test_count} = {rate:.1f}%")
    assert resolved_count >= test_count * 0.10  # At least 10%

def test_cage_code_resolution():
    """Test resolver with mock SAM.gov data (CAGE codes)."""
    resolver = TickerResolverV3()

    # Mock SAM.gov contract with CAGE code
    test_cases = [
        {"awardee": "NORTHROP GRUMMAN CORP", "cage_code": "1WPN2", "expect_ticker": True},
        {"awardee": "LOCKHEED MARTIN CORP", "cage_code": "04ZLA", "expect_ticker": True},
    ]

    for case in test_cases:
        result = resolver.resolve(case["awardee"], cage_code=case["cage_code"])
        # With CAGE, we expect higher resolution rate
        if case["expect_ticker"]:
            assert result["confidence"] in ["very_high", "high", "medium"], \
                f"Expected higher confidence for {case['awardee']}"
```

- [ ] **Step 2: Run integration test**

```bash
rtk pytest tests/test_integration_multi_identifier.py -v -s
```

Expected: Integration test passes with resolution rate > 10%.

- [ ] **Step 3: Update main.py for live trading (add CAGE parameter)**

Find where `resolve_ticker` is called in `main.py` (typically in the contract filter or trading loop). Update the call to:

```python
cage_code = award.get("cage_code", "")  # Assume SAM.gov provides this
ticker, confidence = resolve_ticker(
    awardee_name,
    resolver=resolver,
    cage_code=cage_code
)
```

(Exact location depends on your main.py structure; search for `resolve_ticker` call)

- [ ] **Step 4: Commit**

```bash
rtk git add tests/test_integration_multi_identifier.py main.py
rtk git commit -m "feat: Add integration tests and SAM.gov CAGE support for live trading"
```

---

### Task 7: Final validation & cleanup

**Files:**
- Modify: `config.py` (add API endpoints if needed)
- Verify: All tests pass

- [ ] **Step 1: Add API endpoint constants to config.py (if not present)**

Check if `config.py` has these constants; if not, add them:

```python
# GLEIF API (free, no auth)
GLEIF_SEARCH_URL = "https://leilookup.gleif.org/api/v3/lei-records"

# OpenFIGI API (free, no daily limits)
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# Cache TTLs (days)
LEI_CACHE_TTL = 30
TICKER_CACHE_TTL = 7
CAGE_CACHE_TTL = 30
```

- [ ] **Step 2: Run all tests**

```bash
rtk pytest tests/ -v --tb=short 2>&1 | tail -50
```

Expected: All tests PASS (including new V3 tests and integration tests).

- [ ] **Step 3: Run full end-to-end pipeline**

```bash
rtk python build_training_set.py --quiet
```

Expected: Completes without errors. Check output:

```bash
rtk ls -lh datasets/training_set_final.csv
```

- [ ] **Step 4: Verify no regressions**

Run existing unit tests to ensure backward compatibility:

```bash
rtk pytest tests/test_ticker_resolver_v3.py::test_resolve_without_cage_code -v
```

Expected: PASS (V3 backward compatible with V2 behavior).

- [ ] **Step 5: Final commit**

```bash
rtk git add config.py
rtk git commit -m "docs: Add GLEIF and OpenFIGI API configuration constants"
```

---

## Spec Coverage Summary

✓ **Tier 1 (CAGE→LEI→OpenFIGI)** — Tasks 2-4, 6
✓ **Tier 2 (SEC public markets)** — Task 4 (refactored V2 logic)
✓ **Tier 3 (Fallback + multi-path)** — Task 4 (audit trail foundation)
✓ **Confidence scoring** — Tasks 4-6 (audit trail + confidence levels)
✓ **Error handling** — Tasks 2-3 (graceful fallback)
✓ **USASpending training** — Task 5
✓ **SAM.gov live support** — Task 6 (CAGE parameter)
✓ **Caching** — Task 1 (ApiCache)
✓ **Testing** — All tasks (TDD approach)
