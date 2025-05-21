import os
from importlib import metadata

import pandas as pd


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


def compare_two_dataframes(df1, df2):
    """
    Compare two dataframes and identify the differences between them.

    Parameters:
    - df1: pd.DataFrame
        The first dataframe to compare.
    - df2: pd.DataFrame
        The second dataframe to compare.

    Returns:
    - diff: pd.DataFrame
        A dataframe containing the rows that are different between df1 and df2.
    - changed_columns: list
        A list of column names that have differences between df1 and df2.
    """
    all_columns = df1.columns.union(df2.columns)
    df1_aligned = df1.reindex(columns=all_columns)
    df2_aligned = df2.reindex(columns=all_columns)

    df1_aligned["version"] = "df1"
    df2_aligned["version"] = "df2"

    diff = pd.concat([df1_aligned, df2_aligned]).drop_duplicates(
        subset=all_columns, keep=False
    )

    changed_columns = []
    for column in all_columns:
        if not df1_aligned[column].equals(df2_aligned[column]):
            changed_columns.append(column)

    return diff, changed_columns


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
        "idbr_folder_path"
    )
    snapshot_file_path = os.path.normpath(snapshot_file_path)
    if not snapshot_file_path.endswith(os.sep):
        snapshot_file_path += os.sep
    return snapshot_file_path
