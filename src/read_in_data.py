import json

import pandas as pd  # noqa F401
import pydoop.hdfs as hdfs

folder_path = "/dapsen/workspace_zone/mbs-results/"
file_name = "snapshot-202212-002-2156d36b-e61f-42f1-a0f1-61d1f8568b8e.json"
file_path = folder_path + file_name


def read_json(file_path: str) -> dict:
    """
    This function takes an absolute path on HDFS and returns a dictionary

    Parameters
    ----------
    file_path : str

    Returns
    -------
        dict
    dictionary of data from json file

    """
    with hdfs.open(file_path, "r") as f:
        data = json.load(f)

    return data


# contributors = pd.DataFrame(data["contributors"])
# responses = pd.DataFrame(data["responses"])
