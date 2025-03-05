import json

def merge_two_config_files(user_config_path="config_user.json", constants_config_path="config_constants.json", destination_path=None):
    """
    Load and merge two configuration files: a user-specific config and a constants config.

    Parameters
    ----------
    user_config_path : str, optional
        Path to the user-specific configuration file (default is "config_user.json").
    constants_config_path : str, optional
        Path to the constants configuration file (default is "config_constants.json")..

    Returns
    -------
    Dict
        A dictionary containing the merged configuration.
    """
    
    # Load the user config
    with open(user_config_path, "r") as f:
        user_config  = json.load(f)
        
    # Load the constants config
    with open(constants_config_path, "r") as f:
        constant_config = json.load(f)
        
    # Merge both config files
    config = {**user_config, **constant_config}

    # Dump the config.json in the destination_path
    with open(destination_path, "w") as f:
        json.dump(config, f)