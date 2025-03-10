import json
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from mbs_results.utilities.merge_two_config_files import merge_two_config_files


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

    config = merge_two_config_files(
        config_user_path="mbs_results/config_user.json",
        config_dev_path="mbs_results/config_dev.json",
    )

    # update the file paths with those supplied from the test data
    config.update(file_paths)

    with open("config.json", "w") as f:
        json.dump(config, f)
