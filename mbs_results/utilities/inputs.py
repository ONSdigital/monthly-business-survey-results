import os

import mbs_results
from mbs_results import logger
from mbs_results.utilities.merge_two_config_files import merge_two_config_files


def load_config(config_user_dict=None):
    # Get the directory where mbs_results is installed
    parent_dir = os.path.dirname(mbs_results.__file__)

    # Get config paths
    config_user_path = "config_user.json"
    config_dev_path = os.path.join(parent_dir, "configs", "config_dev.json")

    config = merge_two_config_files(config_user_path, config_dev_path)
    logger.info(
        f"config dictionary created from merging {config_user_path} "
        f"and {config_dev_path}"
    )

    if config_user_dict is not None:
        config.update(config_user_dict)
        logger.info(
            "config dictionary updated with config user dictionary from the testing"
        )

    logger.info("Appending config files with selected sic column name")
    config["finalsel_keep_cols"].append(config["sic"])
    config["population_keep_columns"].append(config["sic"])

    return config
