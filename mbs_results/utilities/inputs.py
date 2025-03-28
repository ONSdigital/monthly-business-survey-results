import getpass
import os

import mbs_results
from mbs_results import logger
from mbs_results.utilities.merge_two_config_files import merge_two_config_files


def replace_username_in_dict(d, username):
    """
    Recursively replaces all instances of {USERNAME} in a dictionary or list with
    the provided username.

    Parameters
    ----------
    d : dict, list, or str
        The input data structure where {USERNAME} should be replaced. Can be a
        dictionary, list, or string.
    username : str
        The username to replace {USERNAME} with.

    Returns
    -------
    dict, list, or str
        The updated data structure with {USERNAME} replaced by the provided username.
    """
    if isinstance(d, dict):
        return {k: replace_username_in_dict(v, username) for k, v in d.items()}
    elif isinstance(d, list):
        return [replace_username_in_dict(i, username) for i in d]
    elif isinstance(d, str):
        return d.replace("{USERNAME}", username)
    else:
        return d


def load_config(config_user_dict=None):
    # Get the directory where mbs_results is installed
    parent_dir = os.path.dirname(mbs_results.__file__)

    # Get config paths
    config_user_path = "./mbs_results/configs/config_user.json"
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

    username = getpass.getuser()
    config = replace_username_in_dict(config, username)

    return config
