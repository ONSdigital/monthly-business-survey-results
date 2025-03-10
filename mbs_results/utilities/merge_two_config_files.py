import json


def merge_two_config_files(
    config_user_path="mbs_results/config_user.json",
    config_dev_path="mbs_results/config_dev.json",
):
    """
    Load and merge two configuration files: a user-specific config and a
    dev-specific config.

    Parameters
    ----------
    config_user_path : str, optional
        Path to the user-specific configuration file
        (default is "mbs_results/config_user.json").
    config_dev_path : str, optional
        Path to the dev-specific configuration file
        (default is "mbs_results/config_dev.json").

    Returns
    -------
    config : Dict
        A dictionary containing the merged configuration.
    """

    with open(config_user_path, "r") as f:
        config_user = json.load(f)

    with open(config_dev_path, "r") as f:
        config_dev = json.load(f)

    config = {**config_user, **config_dev}
    return config
