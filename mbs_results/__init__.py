import logging
import os
import sys
from pathlib import Path

logging_str = (
    "[%(asctime)s: %(name)s: %(levelname)s: %(module)s: "
    "%(funcName)s: %(lineno)d] %(message)s"
)

# Get the root directory by going up two levels
root_dir = Path(__file__).resolve().parents[1]

log_dir = os.path.join(root_dir, "logs")
os.makedirs(log_dir, exist_ok=True)
log_filepath = os.path.join(log_dir, "running_log.log")

logging.basicConfig(
    level=logging.DEBUG,
    format=logging_str,
    handlers=[logging.FileHandler(log_filepath), logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("MBS")
