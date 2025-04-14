"""The ray_logging_filter module provides a mechanism to silence ray's runtime_context warnings"""
import logging
from contextlib import contextmanager


@contextmanager
def suppress_ray_runtime_context_warning_logs():
    """Suppress ray runtime warning logs"""
    logger = logging.getLogger('ray.runtime_context')
    previous_level = logger.getEffectiveLevel()
    logger.setLevel(max(previous_level, logging.WARNING + 1))
    try:
        yield
    finally:
        logger.setLevel(previous_level)
