import logging
import sys
from .config import LOG_FILE, LOG_LEVEL

def setup_logging() -> None:
    LOG_FILE.touch(exist_ok=True)
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler(LOG_FILE)],
    )