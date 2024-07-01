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
        return df
