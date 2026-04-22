"""
Exponential-backoff retry decorator.

Apply to operations that may fail transiently — HTTP calls, Kafka flushes,
Delta writes against a busy table, etc. The decorator does not swallow the
final exception: it re-raises after `max_retries` attempts so callers can
still distinguish hard failures from intermittent ones.
"""
from __future__ import annotations

import functools
import os
import sys
import time
from typing import Callable, Tuple, Type

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from logger import get_logger  # noqa: E402

log = get_logger(__name__)


def retry(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
):
    """Retry the decorated callable on `exceptions` with exponential backoff.

    `max_retries` is the number of *additional* attempts after the first call,
    so the function is invoked at most ``max_retries + 1`` times. The wait
    before attempt N (0-indexed) is ``backoff_factor ** N`` seconds.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_retries:
                        log.error(
                            "Retry exhausted",
                            extra={"extra_data": {
                                "function": func.__name__,
                                "max_retries": max_retries,
                                "error": str(exc),
                            }},
                        )
                        raise
                    wait = backoff_factor ** attempt
                    log.warning(
                        "Retry attempt",
                        extra={"extra_data": {
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "of": max_retries,
                            "wait_seconds": wait,
                            "error": str(exc),
                        }},
                    )
                    time.sleep(wait)
        return wrapper
    return decorator
