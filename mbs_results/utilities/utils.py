import re
from io import BytesIO
from typing import List

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
        Main dataframe with filtered out questions.

    """
    try:
        filter_out_questions_df = pd.read_csv(filter_out_questions_path)

    except FileNotFoundError:
        print(
            """File not found. Please check filter_out_questions_path path,
         filter_out_questions_df is being created by filter_out_questions()
         in mbs_results/staging/data_cleaning.py"""
        )

    df = pd.concat([df, filter_out_questions_df])

    return df
