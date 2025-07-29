import fnmatch
import json
import logging
import uuid
from datetime import datetime
from os import listdir
from os.path import isfile, join
from typing import List

import pandas as pd

from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files


def create_snapshot(
    input_directory: str,
    periods: List[str],
    output_directory: str,
    log_file: str,
    config: dict,
    man_data_path=None,
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
    log_file: str
        File path to log file
    config: dict
        main config file for the pipeline.
    man_data_path: str, optional
        Path for mannual constructions

    Action
    -------
    Writes a json file in desired location that looks like a SPP snapshot

    Example
    -------
    >>periods = [str(i) for i in range(202201, 202213)] + ["202301", "202302", "202303"]
    >>input_directory = "path/mbs_anonymised_2024"
    >>output_directory = "path/mbs-data"
    >>log_file = "example_log.log"
    >>create_snapshot(input_directory, periods, output_directory, log_file, config)
    """

    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info(f"Concatenating qv files from {input_directory}")
    qv_df = concat_files_from_pattern(input_directory, "qv*.csv", periods)

    if man_data_path:
        qv_df = remove_mannual_constructions(qv_df, man_data_path, config)

    logger.info(f"Concatenating cp files from {input_directory}")
    cp_df = concat_files_from_pattern(input_directory, "cp*.csv", periods)

    qv_df_validated = validate_nil_markers(cp_df, qv_df, logger)

    responses = convert_qv_to_responses(qv_df_validated)
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

    logger.info(f"Writting snapshot to {output_directory} for periods {periods}")
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
            x["error_mkr"] if x["response_type"] == 2 else str(x["response_type"])
        ),
        axis=1,
    )

    error_marker_map = {
        "C": ("Clear", "211"),
        "O": ("Clear - overridden", "210"),
        "E": ("Check needed", "201"),
        "W": ("Check needed", "201"),
        "1": ("Form sent out", "100"),
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
            config["sic"],
            "frotover",
        ]
    ]
    finalsel_data["formtype"] = "0" + finalsel_data["formtype"].astype(str)
    finalsel_data.rename(columns=finalsel_column_remapper, inplace=True)
    return df.merge(finalsel_data, on=["reference", "period"], how="left")


def validate_nil_markers(
    cp_df: pd.DataFrame, qv_df: pd.DataFrame, logger: logging.Logger
) -> pd.DataFrame:
    """
    Validates and adjusts 'adjusted_value' in the dataframe based on 'response_type'.

    If 'response_type' is greater than or equal to 4 and 'adjusted_value' is not 0,
    sets 'adjusted_value' to 0 and logs the details of the adjustment.

    Parameters
    ----------
    cp_df : pd.DataFrame
        The dataframe containing response_type
    qv_df : pd.DataFrame
        The dataframe containing adjusted_value
    logger : logging.Logger
        The logger object used to log details of the adjustments.

    Returns
    -------
    pd.DataFrame
        The dataframe with 'adjusted_value' set to 0 where necessary.
    """
    qv_cp_df = qv_df.merge(
        cp_df[["reference", "period", "response_type"]],
        on=["reference", "period"],
        how="left",
    )

    condition = (qv_cp_df["response_type"] >= 4) & (qv_cp_df["adjusted_value"] != 0)
    filtered_qv_df = qv_cp_df[condition]

    for index, row in filtered_qv_df.iterrows():
        qv_cp_df.loc[index, "adjusted_value"] = 0
        logger.warning(
            f"Adjusted value set to 0 for: reference {row['reference']}, "
            f"period {row['period']}, question number {row['question_no']}, "
            f"with response type {row['response_type']}."
        )

    validated_qv_df = qv_cp_df.drop(columns=["response_type"])

    return validated_qv_df


def remove_mannual_constructions(
    qv_df: pd.DataFrame, man_file_path: str, config: dict
) -> pd.DataFrame:
    """
    Removes mannual constructions values from qv files based on index

    Parameters
    ----------
    qv_df : pd.DataFrame
        dataframe from qv files.
    man_df : pd.DataFrame
        dataframe with mannual construction files.
    config : dict
        pipeline settings dictionary.

    Returns
    -------
    qv_df : pd.DataFrame
        Qv files without mannual constructions.

    """
    man_df = pd.read_csv(man_file_path)

    qv_df = qv_df.copy()

    man_df.rename(columns={config["question_no"]: "question_no"}, inplace=True)

    man_df.set_index(
        [config["period"], config["reference"], "question_no"], inplace=True
    )

    qv_df.set_index(["period", "reference", "question_no"], inplace=True)

    rows_ro_remove = qv_df.index.intersection(man_df.index)

    qv_df.drop(rows_ro_remove, inplace=True)

    qv_df.reset_index(inplace=True)

    return qv_df
