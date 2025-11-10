"""Small utility helpers: logger factory and retry decorator.

Keep this minimal and dependency-free for easy reuse from scripts.
"""
import logging
import os
import time
import functools


def get_logger(name: str = __name__):
    """Return a configured logger. Level is controlled by LOG_LEVEL env var."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    # configure root logger once
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger(name)


def retry(on_exception=Exception, tries: int = 3, delay: float = 2.0, backoff: float = 2.0):
    """A simple retry decorator with exponential backoff.

    Usage:
        @retry(tries=3)
        def call():
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _tries = tries
            _delay = delay
            while _tries > 0:
                try:
                    return func(*args, **kwargs)
                except on_exception:
                    _tries -= 1
                    if _tries <= 0:
                        raise
                    time.sleep(_delay)
                    _delay *= backoff

        return wrapper

    return decorator
