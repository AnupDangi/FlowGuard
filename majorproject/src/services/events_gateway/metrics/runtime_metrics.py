"""In-process runtime counters for lightweight monitoring."""

from collections import defaultdict
from threading import Lock

_COUNTERS = defaultdict(int)
_LOCK = Lock()


def inc_counter(name: str, value: int = 1) -> None:
    with _LOCK:
        _COUNTERS[name] += value


def get_counters() -> dict:
    with _LOCK:
        return dict(_COUNTERS)
