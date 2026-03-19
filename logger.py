"""
logger.py
=========
Import this in any file and just use:

    from logger import logger

    logger.info("message here")
    logger.warning("something wrong")
    logger.error("something failed")

That's it. Nothing else needed.

LOG FILE:
    pipeline.log — created in same folder as this file
    mode = "w"   — every new run overwrites previous log
                   old log is gone, fresh log begins

TWO OUTPUTS SIMULTANEOUSLY:
    1. pipeline.log file  (inside container)
    2. Terminal / Docker Desktop console (live)
"""

import logging
import os
import sys

# ==============================================================================
# PATH
# ==============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "pipeline.log")


# ==============================================================================
# LOGGER
# ==============================================================================

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

# Prevent duplicate logs when Streamlit re-imports this module
if logger.handlers:
    logger.handlers.clear()


# ==============================================================================
# HANDLER 1 — FILE
# mode="w" = overwrite on every run (fresh log every time)
# Change to mode="a" if you want to keep history
# ==============================================================================

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.INFO)


# ==============================================================================
# HANDLER 2 — TERMINAL
# ==============================================================================

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)


# ==============================================================================
# FORMAT
# 10:23:01 | INFO    | your message here
# 10:23:04 | WARNING | something went wrong
# ==============================================================================

formatter = logging.Formatter(
    fmt     = "%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt = "%H:%M:%S"
)

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)


# ==============================================================================
# ATTACH
# ==============================================================================

logger.addHandler(file_handler)
logger.addHandler(console_handler)


# ==============================================================================
# SILENCE NOISY THIRD PARTY LIBRARIES
# These spam the log with their own internal debug messages
# ==============================================================================

for _lib in ["google", "google.genai", "httpx", "httpcore", "urllib3", "asyncio", "streamlit"]:
    logging.getLogger(_lib).setLevel(logging.CRITICAL)