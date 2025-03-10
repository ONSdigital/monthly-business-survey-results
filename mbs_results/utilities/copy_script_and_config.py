import os
import shutil
import json

import mbs_results
from mbs_results.utilities.merge_two_config_files import merge_two_config_files
from mbs_results import logger


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

    # Append main.py and config.json
    config_user_path = os.path.join(target_path, "config_user.json")
    config_dev_path = os.path.join(target_path, "config_dev.json")
    
    # Merge the config_user.json and config_dev.json
    # into config.json in the mbs_results dir
    config = merge_two_config_files(config_user_path, config_dev_path)
    logger.info(f"{config_path} created from merging {config_user_path} and {config_dev_path}")
    
    # Dump the config.json  
    with open(config_path, "w") as f:
        json.dump(config, f)

    # Get the destination for the copy
    working_directory = os.getcwd()

    # Copy the files
    shutil.copy(main_path, working_directory)
    shutil.copy(config_path, working_directory)
    
    # remove the config.json. 
    # Note that config. json = concatenation of config_user.json and config_dev.json
    os.remove(config_path)
