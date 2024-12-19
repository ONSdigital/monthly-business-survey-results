import os
import shutil

def move_files(mbs_results_install_location):
    """
    Copy mbs_results/main.py and mbs_results/config.json from
    virtual environment site packages into the current working directory
    """

    # Get the directory where mbs_results is installed
    target_path = os.path.dirname(mbs_results_install_location)

    # Append main.py and config.json
    main_path = os.path.join(target_path, "main.py")
    config_path = os.path.join(target_path, "config.json")

    # Get the destination for the copy
    working_directory = os.getcwd()

    # Copy the files
    shutil.copy(main_path, working_directory)
    shutil.copy(config_path, working_directory)