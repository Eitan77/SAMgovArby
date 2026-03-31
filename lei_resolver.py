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
