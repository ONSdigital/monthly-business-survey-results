import fnmatch
import json
import uuid
from datetime import datetime
from os import listdir
from os.path import isfile, join
from typing import List

import pandas as pd

from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files


def create_snapshot(
    input_directory: str, periods: List[str], output_directory: str, config: dict
):
    """
    Reads qv and cp files, applies transformations and writes snapshot.

    Parameters
    ----------
    input_directory : str
        Folder path to CSW files.
    periods: List[str]
        list of periods to include in the snapshot
    output_directory : str
        Folder path to write the snapshot.
    config: dict
        main config file for the pipeline.


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

    contributors_with_finalsel = load_and_join_finalsel(
        contributors,
        input_directory + "finalsel*",
        config["sample_column_names"],
        config,
    )

    output = {
        "snapshot_id": input_directory + str(uuid.uuid4().hex),
        "contributors": contributors_with_finalsel.to_dict("list"),
        "responses": responses.to_dict("list"),
    }

    max_period = max([int(period) for period in periods])

    with open(
        f"{output_directory}snapshot_qv_cp_{max_period}_{len(periods)}.json",
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


def convert_cp_to_contributors(df: pd.DataFrame) -> pd.DataFrame:
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
        # Error marker mapping for cases when response <= 2
        "C": ("Clear - overridden", "211"),
        "O": ("Clear", "210"),
        "E": ("Check needed", "201"),
        "W": ("Check needed", "201"),
        # Check where codes 200 - 900 come from
        # We believe this are not used in MBS.
        # Need to create a validation check to ensure that these are not needed
        # Adding this task to the backlog
        "200": ("Form sent out", "100"),
        "300": (
            "Clear - overridden",
            "211",
        ),  # Out of scopes have data in them for BERD!
        "500": ("Check needed", "201"),
        "600": ("Clear", "210"),
        "700": (
            "Clear - overridden",
            "211",
        ),  # Out of scopes have data in them for BERD!
        "800": ("Clear - overridden", "211"),
        "900": (
            "Clear - overridden",
            "211",
        ),  # Out of scopes have data in them for BERD!
        # Response type mapping
        "5": ("Combined child (NIL2)", "302"),
        "6": ("Out of scope (NIL3)", "303"),
        "7": ("Ceased trading (NIL4)", "304"),
        "8": ("Dormant (NIL5)", "305"),
        "12": ("Part year return (NIL8)", "308"),
        "13": ("No UK activity (NIL9)", "309"),
    }

    df["status"] = df["combined_error_marker"].map(error_marker_map)
    df[["status", "statusencoded"]] = pd.DataFrame(
        df["status"].tolist(), index=df.index
    )

    df["createdby"] = "csw to spp converter"
    df["createddate"] = datetime.today().strftime("%d/%m/%Y")

    return df[
        ["period", "reference", "status", "statusencoded", "createdby", "createddate"]
    ]


def convert_qv_to_responses(df: pd.DataFrame) -> pd.DataFrame:
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
        "question_no": "questioncode",
        "returned_value": "response",
        "adjusted_value": "adjustedresponse",
    }
    df["createdby"] = "csw to spp converter"
    df["createddate"] = datetime.today().strftime("%d/%m/%Y")

    out_columns = ["reference", "period", "createdby", "createddate"] + list(
        rename_columns.keys()
    )
    return df[out_columns].rename(columns=rename_columns)


def load_and_join_finalsel(
    df: pd.DataFrame, finalsel_path: str, finalsel_cols: list, config: dict
) -> pd.DataFrame:
    """
    Loads finalsel data and joins it with the input dataframe.
    NOTE: This function may not be needed if input data is adjusted
    If we change the columns loaded from json files, this function could be removed

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to join with finalsel data.
    finalsel_path: str
        Path to finalsel file.
    finalsel_cols: List[str]
        List containing all column names in the finalsel file

    Returns
    -------
    pd.DataFrame
        Dataframe that is a join of input dataframe and finalsel data.
    """
    finalsel_column_remapper = {
        "cell_no": "cellnumber",
        "froempees": "frozenemployees",
        "frosic2007": "frozensic",
        "frotover": "frozenturnover",
    }
    finalsel_data = read_and_combine_colon_sep_files(
        finalsel_path, finalsel_cols, config
    )
    finalsel_data = finalsel_data[
        [
            "reference",
            "period",
            "cell_no",
            "formtype",
            "froempees",
            "frosic2007",
            "frotover",
        ]
    ]
    finalsel_data["formtype"] = "0" + finalsel_data["formtype"].astype(str)
    finalsel_data.rename(columns=finalsel_column_remapper, inplace=True)
    return df.merge(finalsel_data, on=["reference", "period"], how="left")
