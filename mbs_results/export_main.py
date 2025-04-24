"""Main pipeline file for the exporting of files"""

import logging
from importlib import reload

from mbs_results.outputs import export_files

logging.basicConfig(level=logging.INFO)

reload(export_files)

export_config_path = "mbs_results/configs/config_export.json"

if __name__ == "__main__":
    export_files.run_export(export_config_path)
