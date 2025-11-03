"""Main pipeline file for the exporting of files"""

import logging
from importlib import reload

from mbs_results.outputs import export_files

logging.basicConfig(level=logging.INFO)

reload(export_files)

if __name__ == "__main__":
    export_files.run_export("config_export.json")
