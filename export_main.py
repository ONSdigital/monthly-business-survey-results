"""Main pipeline file for the exporting of files"""

#  import os
from importlib import reload

from mbs_results.outputs import export_files
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import validate_config

# my_wd = os.getcwd()
# print(f"Starting the program in {my_wd}")
# my_repo = "monthly-business-survey-results"
# if not my_wd.endswith(my_repo):
#     os.chdir("..")
#     my_wd = os.getcwd()
#     print(f"Changed wd to {my_wd}")


reload(export_files)

if __name__ == "__main__":
    config = load_config()
    validate_config(config)
    export_files.run_export(config)
