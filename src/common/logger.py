import logging
import os

# Define project logger
logger = logging.getLogger(__name__)

_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(levelname)s %(message)s")
_handler.setFormatter(_formatter)

logger.addHandler(_handler)

if os.environ.get("DEBUG_LOG") == "true":
    logger.setLevel(level=10)
else:
    logger.setLevel(level=20)
