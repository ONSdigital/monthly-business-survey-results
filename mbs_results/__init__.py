import os
import sys
import logging
from from_root import from_root

logging_str = ("[%(asctime)s: %(name)s: %(levelname)s: %(module)s: "
               "%(funcName)s: %(lineno)d] %(message)s")

log_dir = os.path.join(from_root(), "logs")
os.makedirs(log_dir, exist_ok=True)
log_filepath = os.path.join(log_dir, "running_log.log")

logging.basicConfig(
    level= logging.INFO,
    format= logging_str,

    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("MBS")