import json
import logging

logger = logging.getLogger(__name__)


def load_config(config_file_path):
    with open(config_file_path, "r") as f:
        return json.load(f)


def merge_two_config_files(
    config_user_path="config_user.json",
    config_dev_path="mbs_results/configs/config_dev.json",
    config_user_dict=None,
):
    """
    Load and merge two configuration files: a user-specific config and a
    dev-specific config.

    Parameters
    ----------
    config_user_path : str, optional
        Path to the user-specific configuration file
        (default is "config_user.json").
    config_dev_path : str, optional
        Path to the dev-specific configuration file
        (default is "mbs_results/configs/config_dev.json").
    config_user_dict : dict, optional
        The user config if it has already been read in or
        defined as a dict for testing purposes.

    Returns
    -------
    config : Dict
        A dictionary containing the merged configuration.
    """
    if not config_user_dict:
        try:
            config_user = load_config(config_user_path)
            logger.info(f"Loaded user config from {config_user_path}")
        except Exception as e:
            logger.error(f"Error loading user config from {config_user_path}: {e}")
            config_user = {}
    else:
        config_user = config_user_dict

    try:
        config_dev = load_config(config_dev_path)
        logger.info(f"Loaded dev config from {config_dev_path}")
    except Exception as e:
        logger.error(f"Error loading dev config from {config_dev_path}: {e}")
        config_dev = {}

    config = {**config_user, **config_dev}
    return config
