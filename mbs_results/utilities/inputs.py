import json
from mbs_results import logger


def load_config(path="config.json"):
    try:
        with open(path, "r") as f:
            config = json.load(f)
        logger.info(f'Successfully loaded the config file from path: {path}')
    except FileNotFoundError as e:
        logger.error(f"if {path} is not found, run the setup_mbs to create {path} from merging the two JSON files")
        raise e
    
    config["mbs_results_path"] = config["folder_path"] + config["mbs_file_name"]
    
    return config
