"""Cache raw SAM.gov awards to JSON for faster re-use."""
import json
import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".award_cache")


def get_cache_file(start_date: str, end_date: str) -> str:
    """Get the cache filename for a date range."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    start = start_date.replace("-", "")
    end = end_date.replace("-", "")
    return os.path.join(CACHE_DIR, f"awards_{start}_{end}.json")


def load_from_cache(start_date: str, end_date: str):
    """Load cached awards if available. Returns list or None."""
    cache_file = get_cache_file(start_date, end_date)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                awards = json.load(f)
            log.info(f"Loaded {len(awards)} awards from cache ({cache_file})")
            return awards
        except Exception as e:
            log.warning(f"Cache read failed: {e}")
    return None


def save_to_cache(awards: list, start_date: str, end_date: str):
    """Save awards to cache."""
    cache_file = get_cache_file(start_date, end_date)
    try:
        with open(cache_file, "w") as f:
            json.dump(awards, f, indent=2)
        log.info(f"Cached {len(awards)} awards to {cache_file}")
    except Exception as e:
        log.warning(f"Cache write failed: {e}")


def clear_cache(start_date: str = None, end_date: str = None):
    """Clear all cached files, or specific range if dates provided."""
    if start_date and end_date:
        cache_file = get_cache_file(start_date, end_date)
        if os.path.exists(cache_file):
            os.remove(cache_file)
            log.info(f"Cleared cache for {start_date} → {end_date}")
    else:
        for f in os.listdir(CACHE_DIR):
            os.remove(os.path.join(CACHE_DIR, f))
        log.info("Cleared all caches")
