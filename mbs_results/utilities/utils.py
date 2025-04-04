import os
import re
from importlib import metadata
from io import BytesIO
from typing import List

import pandas as pd

from mbs_results.utilities.file_selector import find_files


def convert_column_to_datetime(dates):
    """
    Convert string pandas series to datetime (from raw inputs).

    Parameters
    ----------
    dates : pd.Series.

    Returns
    -------
    df : pd.Series
    """
    return pd.to_datetime(dates, format="%Y%m")


def read_colon_separated_file(
    filepath: str, column_names: List[str], period="period"
) -> pd.DataFrame:
    """
    Read data stored as text file, columns separated by colon and any amount of
    white space, and return the data as a dataframe with an additional column
    containing the date derived from the six numbers at the end of the filename,
    preceded by an underscore, eg `_202401`.

    Parameters
    ----------
    filepath : str
        location of data file to read
    column_names : List[str]
        list of column names in data file

    Return
    ------
    pd.DataFrame
    """
    with open(filepath, mode="rb") as file:
        buffer = BytesIO(file.read())
        df = pd.read_csv(buffer, sep=r"\s*:\s*", names=column_names, engine="python")
        date_string = re.findall(r"_(\d{6})", filepath)
        df[period] = int(date_string[0])

    return df


def append_filter_out_questions(
    df: pd.DataFrame, filter_out_questions_path: str
) -> pd.DataFrame:
    """Appends data with question codes which were ommitted from the processing

    Parameters
    ----------
    df : pd.DataFrame
        Main dataframe to append.
    filter_out_questions_path : str
        File path with filtered out questions.

    Returns
    -------
    df : pd.DataFrame
       "Main dataframe with filtered-out questions. If the file is not found or empty,
        logs a FileNotFoundError error and returns original dataframe"

    """
    try:
        filter_out_questions_df = pd.read_csv(filter_out_questions_path)

        df = pd.concat([df, filter_out_questions_df])

    except FileNotFoundError:
        print(
            """File not found. Please check filter_out_questions_path path,
         filter_out_questions_df is being created by filter_out_questions()
         in mbs_results/staging/data_cleaning.py"""
        )

    return df


def get_versioned_filename(prefix, config):

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    filename = f"{prefix}_v{file_version_mbs}_{snapshot_name}.csv"

    return filename


def read_and_combine_colon_sep_files(column_names: list, config: dict) -> pd.DataFrame:
    """
    reads in and combined colon separated files from the specified folder path

    Parameters
    ----------
    folder_path : str
        folder path containing the colon separated files
    column_names : list
        list of column names in colon separated file
    config : dict
        main pipeline config containing period column name

    Returns
    -------
    pd.DataFrame
        combined colon separated files returned as one dataframe.
    """
    sample_files = find_files(
        file_path=config["folder_path"],
        file_prefix=config["sample_prefix"],
        current_period=config["current_period"],
        revision_window=config["revision_window"],
        config=config,
    )

    df = pd.concat(
        [
            read_colon_separated_file(f, column_names, period=config["period"])
            for f in sample_files
        ],
        ignore_index=True,
    )
    return df


def get_snapshot_alternate_path(config):
    """
    Check if snapshot_alternate_path is provided in the config and use this to load the
    snapshot. If snapshot_alternate_path is not provided, snapshot will be loaded from
    the folder_path.
    Also checks that folder path ends in a slash, appends one if not included.
    Does not overwrite the folder_path in the config.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing the snapshot_alternate_path and folder_path
        keys

    Returns
    -------
    str
        The path to the folder where the snapshot is located. If snapshot_alternate_path
        is not provided, returns the folder_path.
    """

    snapshot_file_path = config.get("snapshot_alternate_path_OPTIONAL") or config.get(
        "folder_path"
    )
    snapshot_file_path = os.path.normpath(snapshot_file_path)
    if not snapshot_file_path.endswith(os.sep):
        snapshot_file_path += os.sep
    return snapshot_file_path


if __name__ == "__main__":
    # Example usage
    from mbs_results.utilities.inputs import load_config

    config = load_config(None)

    returned = get_snapshot_alternate_path(config)
    print(returned)
