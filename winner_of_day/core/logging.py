from __future__ import annotations

import logging

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"


def setup_logging(level: int = logging.DEBUG) -> logging.Logger:
    logging.basicConfig(level=level, format=LOG_FORMAT)
    return logging.getLogger("winner_of_day_bot")
