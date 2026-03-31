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
