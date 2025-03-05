import json
from contextlib import contextmanager
from pathlib import Path

import pandas as pd


def load_and_format(filename):
    """Load csv as pandas dataframe and cast period column to datetime type"""
    df_loaded = pd.read_csv(filename)
    df_loaded["period"] = pd.to_datetime(df_loaded["period"], format="%Y%m")
    return df_loaded


def load_filter(filter_path):
    """Check if path exists and load csv as pandas dataframe"""
    my_file = Path(filter_path)

    if my_file.is_file():

        df = pd.read_csv(my_file)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m")

        return df


# when updating to python>=3.7 this can be replaced by importing
# contextlib.nullcontext as does_not_raise
@contextmanager
def does_not_raise():
    yield


def create_testing_config(file_paths):
    """Copy config to testing directory and update file_paths"""

    # merger the two config files into config.json
    # Load the user config
    with open("mbs_results/config_user.json", "r") as f:
        user_config = json.load(f)

    # Load the constants config
    with open("mbs_results/config_constants.json", "r") as f:
        constant_config = json.load(f)

    # Merge both config files
    config = {**user_config, **constant_config}

    config.update(file_paths)

    with open("config.json", "w") as f:

        json.dump(config, f)
