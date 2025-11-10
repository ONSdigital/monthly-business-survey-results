"""Main pipeline file for the exporting of files"""

import logging
from importlib import reload

from mbs_results.outputs import export_files

logging.basicConfig(level=logging.INFO)

reload(export_files)


def run_export_wrapper():
    """
    Wrapper function for run export

    This will allow it to run via console script or as an executable script.

    Requires a filled config_export.json file in the working directory.
    """
    export_files.run_export("config_export.json")


if __name__ == "__main__":

    run_export_wrapper()
