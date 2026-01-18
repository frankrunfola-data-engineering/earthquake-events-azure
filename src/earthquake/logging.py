from __future__ import annotations
import logging


def configure_logging(log_level: str | int = "INFO") -> logging.Logger:
    """Configure a consistent logger for CLI + notebooks.

    Safe to call multiple times.
    """
    logger = logging.getLogger("earthquake")
    if logger.handlers:
        return logger

    if isinstance(log_level, str):
        level_upper = log_level.strip().upper()
        numeric_level = logging.getLevelName(level_upper)
        if isinstance(numeric_level, str):
            numeric_level = logging.INFO
    else:
        numeric_level = int(log_level)

    logger.setLevel(numeric_level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
