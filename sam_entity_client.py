"""SAM.gov Entity Information API client.

Resolves CAGE codes to canonical legal business names using the authoritative
SAM.gov entity registry. This is the correct way to use CAGE codes as lookup
keys — SAM.gov issued the CAGE codes, so SAM.gov is the ground truth for what
company a given CAGE code belongs to.

The canonical legal name returned by this client is the name the company used
when registering with the federal government, and is the most standardized form
of the name — most likely to match SEC EDGAR records exactly.

Rate limiting note: The SAM.gov Entity API allows roughly 1,000 requests/day
on a standard API key. With tens of thousands of unique CAGE codes in a training
set, the first build will exhaust the daily quota quickly. The client handles
this with a circuit breaker: after 5 consecutive 429s it stops making API calls
for the rest of the process session. Cached hits (from prior runs) still work
normally. On subsequent builds the cache covers most CAGE codes.

API docs: https://open.gsa.gov/api/entity-api/
"""
import logging
import os
import time
import requests
from api_cache import ApiCache
from rate_limiter import RateLimiter
from config import SAM_GOV_API_KEY, user_cache_dir

log = logging.getLogger(__name__)

SAM_ENTITY_URL = "https://api.sam.gov/entity-information/v3/entities"

_entity_cache = ApiCache(cache_file=os.path.join(user_cache_dir(), "sam_entity_cache.json"))
_rate_limiter = RateLimiter(min_interval=2.0)  # conservative: 1 req / 2 sec

# Circuit breaker: shared across all SamEntityClient instances in a process.
# After _CB_THRESHOLD consecutive 429s we disable live lookups for the session.
_CB_THRESHOLD = 5
_consecutive_429s = 0
_circuit_open = False  # True = disabled for this session


class SamEntityClient:
    """Look up CAGE codes via SAM.gov Entity Information API."""

    def __init__(self, api_key: str | None = None, cache: ApiCache | None = None):
        self.api_key = api_key or SAM_GOV_API_KEY
        self.cache = cache or _entity_cache
        self._limiter = _rate_limiter

    def lookup_cage(self, cage_code: str) -> dict | None:
        """Return canonical entity data for a CAGE code, or None on failure.

        Result dict keys:
          legal_name  — canonical legal business name from SAM.gov
          uei         — Unique Entity ID (SAM.gov identifier)
          cage_code   — the queried CAGE code (uppercased)

        Successful results and confirmed "not found" results are cached for
        30 days. 429 / transient errors are NOT cached so they retry next run.
        """
        if not cage_code or not cage_code.strip():
            return None

        cage_upper = cage_code.strip().upper()
        cache_key = f"sam_entity:{cage_upper}"

        # Cache hit — always use regardless of circuit state
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        # Circuit open — quota exhausted for this session, skip live lookup
        global _circuit_open
        if _circuit_open:
            return None

        result, cacheable = self._query(cage_upper)

        # Only persist to disk when the result is definitive (success or not-found).
        # 429 / transient errors return cacheable=False so the next run retries.
        if cacheable:
            self.cache.set(cache_key, result, ttl_days=30)

        return result

    def _query(self, cage_code: str) -> tuple[dict | None, bool]:
        """Return (result, cacheable).

        cacheable=True  → write result to disk cache (success or confirmed not-found)
        cacheable=False → don't cache (rate-limit or transient error)
        """
        global _consecutive_429s, _circuit_open

        if not self.api_key:
            log.warning("SAM Entity API: no API key configured — skipping live lookups")
            _circuit_open = True
            return None, False

        self._limiter.wait()
        try:
            params = {
                "cageCode": cage_code,
                "api_key": self.api_key,
                "includeSections": "entityRegistration",
            }
            resp = requests.get(SAM_ENTITY_URL, params=params, timeout=15)

            if resp.status_code == 429:
                _consecutive_429s += 1
                if _consecutive_429s >= _CB_THRESHOLD:
                    _circuit_open = True
                    log.warning(
                        f"SAM Entity API: daily quota exhausted after {_consecutive_429s} "
                        "consecutive 429s — disabling live lookups for this session. "
                        "Resolver will use EDGAR/GLEIF tiers. Cache hits still work."
                    )
                else:
                    log.debug(f"SAM Entity API: 429 rate limited (#{_consecutive_429s}), backing off")
                    time.sleep(5 * _consecutive_429s)  # escalating back-off
                return None, False  # don't cache — retry next run

            # Reset circuit on any non-429 response
            _consecutive_429s = 0

            if resp.status_code == 403:
                log.warning("SAM Entity API: 403 Forbidden — check SAM_GOV_API_KEY")
                _circuit_open = True
                return None, False

            if resp.status_code != 200:
                log.debug(f"SAM Entity API HTTP {resp.status_code} for CAGE {cage_code}")
                return None, False

            data = resp.json()
            entities = data.get("entityData", [])
            if not entities:
                log.debug(f"SAM Entity API: no entity found for CAGE {cage_code}")
                return None, True  # definitive not-found — safe to cache

            reg = entities[0].get("entityRegistration", {})
            legal_name = reg.get("legalBusinessName", "").strip()
            uei = reg.get("ueiSAM", "").strip()

            if not legal_name:
                return None, True

            log.debug(f"SAM Entity API: CAGE {cage_code} → '{legal_name}'")
            return {"legal_name": legal_name, "uei": uei, "cage_code": cage_code}, True

        except requests.Timeout:
            log.debug(f"SAM Entity API timeout for CAGE {cage_code}")
            return None, False
        except Exception as e:
            log.debug(f"SAM Entity API error for CAGE {cage_code}: {e}")
            return None, False
