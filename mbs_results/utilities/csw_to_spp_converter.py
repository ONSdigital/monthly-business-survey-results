import fnmatch
import json
import uuid
from os import listdir
from os.path import isfile, join
from typing import List

import pandas as pd


def create_snapshot(
    input_directory: str, periods: List[str], output_directory: str
) -> pd.DataFrame:
    """
    Reads qv and cp files, applies transformations and writes snapshot.

    Parameters
    ----------
    input_directory : str
        Folder path to CSW files.
    periods: List[str]
        list of periods to include in the snapshot

    Action
    -------
    Writes a json file in desired location that looks like a SPP snapshot

    Example
    -------
    >>periods = [str(i) for i in range(202201, 202213)] + ["202301", "202302", "202303"]
    >>input_directory = "path/mbs_anonymised_2024"
    >>output_directory = "path/mbs-data"
    >>create_snapshot(input_directory, periods, output_directory)
    """

    qv_df = concat_files_from_pattern(input_directory, "qv*.csv", periods)
    cp_df = concat_files_from_pattern(input_directory, "cp*.csv", periods)

    responses = convert_qv_to_responses(qv_df)
    contributors = convert_cp_to_contributors(cp_df)

    output = {
        "id": input_directory + str(uuid.uuid4().hex),
        "contributors": contributors.to_dict("list"),
        "responses": responses.to_dict("list"),
    }

    max_period = max([int(period) for period in periods])

    with open(
        f"{output_directory}/snapshot_qv_cp_{max_period}_{len(periods)}.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(output, f, ensure_ascii=False, indent=4)


def concat_files_from_pattern(
    directory: str, pattern: str, periods: List[str]
) -> pd.DataFrame:
    """
    Loads as pd dataframe of all csv files with pattern and with periods specified
    in periods.

    Parameters
    ----------
    directory : str
        Folder path to CSW files.
    pattern : str
        Regex pattern to filter files in the folder based on name.
    periods: List[str]
        list of periods to include in the snapshot

    Returns
    -------
    pd.DataFrame
        Dataframe containg data from all selected files.
    """

    filenames = [
        filename
        for filename in listdir(directory)
        if ((isfile(join(directory, filename))) & (filename[-10:-4] in periods))
    ]

    filenames = fnmatch.filter(filenames, pattern)
    df_list = [pd.read_csv(directory + "/" + filename) for filename in filenames]
    df = pd.concat(df_list, ignore_index=True)

    return df


def convert_cp_to_contributors(df):
    """
    Converts a dataframe from a cp file from CSW and returns a dataframe that
    looks like a contributors table in from an SPP snapshot.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame from a cp file

    Returns
    -------
    pd.DataFrame
        Dataframe that looks like a contributors table from a snapshot.
    """

    df["combined_error_marker"] = df.apply(
        lambda x: (
            x["error_mkr"] if x["response_type"] <= 2 else str(x["response_type"])
        ),
        axis=1,
    )

    error_marker_map = {
        "C": "Clear",
        "O": "Clear - overridden",
        "E": "Check needed",
        # TODO: Should W map to check needed or something else?
        "W": "Check needed",
        # TODO: Check which ones below are used in SPP
        "3": "Sample deletion",
        "4": "Nil1, dead letter",
        "5": "Nil2, combined return, zero response",
        "6": "Nil3, out-of-scope",
        "7": "Nil4, ceased trading",
        "8": "Nil5, dormant",
        "9": "Nil6, out-of-scope and insufficient data",
        "10": "Nil7, in-scope but suspect data",
        "11": "Dead",
        "12": "Nil8, part year return, death in year",
        "13": "Nil9, out of scope and no UK activity",
    }

    df["status"] = df["combined_error_marker"].map(error_marker_map)

    return df[["period", "reference", "status"]]


def convert_qv_to_responses(df):
    """
    Converts a dataframe from a qv file from CSW and returns a dataframe that
    looks like a responses table in from an SPP snapshot.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame from a qv file

    Returns
    -------
    pd.DataFrame
        Dataframe that looks like a responses table from a snapshot.
    """

    rename_columns = {
        "question_no": "questionnumber",
        "returned_value": "response",
        "adjusted_value": "adjustedresponse",
    }

    out_columns = ["reference"] + list(rename_columns.keys())

    return df[out_columns].rename(rename_columns)
