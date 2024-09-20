import json


def load_config(path="config.json"):
    with open(path, "r") as f:
        config = json.load(f)
    config["mbs_results_path"] = config["folder_path"] + config["mbs_file_name"]

    return config
