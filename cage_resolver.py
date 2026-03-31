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
