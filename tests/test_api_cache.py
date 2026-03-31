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
