"""Thread-safe rate limiter for API request throttling."""
import threading
import time


class RateLimiter:
    """Ensures a minimum interval between calls across threads."""

    def __init__(self, min_interval: float):
        self._min_interval = min_interval
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last = time.time()
