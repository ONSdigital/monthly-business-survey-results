"""Main pipeline file for the exporting of files"""

# import os
from importlib import reload

from mbs_results.outputs import export_files
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import validate_config

# my_wd = os.getcwd()
# print(f"Starting the program in {my_wd}")
# my_repo = r"D:\repos\monthly-business-survey-results"
# if not my_wd.endswith(my_repo):
#     os.chdir("..")


reload(export_files)


def read_validate_config():
    """Read and validate the config."""
    config = load_config()
    validate_config(read_validate_config())
    return config


if __name__ == "__main__":
    myconfig = read_validate_config()
    export_files.run_export(myconfig)

# %%
