import os
import shutil

import mbs_results


def copy_script_and_config():
    """
    Copy mbs_results/main.py and mbs_results/config.json from
    virtual environment site packages into the current working directory
    This uses the installed package to find the location of files.

    Examples
    ---------
    >>> from mbs_results.utilities.copy_script_and_config import copy_script_and_config
    ...
    >>> copy_script_and_config()

    """

    # Get the directory where mbs_results is installed
    target_path = os.path.dirname(mbs_results.__file__)

    # Append main.py and config.json
    main_path = os.path.join(target_path, "main.py")
    config_path = os.path.join(target_path, "config.json")

    # Get the destination for the copy
    working_directory = os.getcwd()

    # Copy the files
    shutil.copy(main_path, working_directory)
    shutil.copy(config_path, working_directory)
