import glob
import warnings

import pandas as pd

from mbs_results.staging.back_data import append_back_data
from mbs_results.staging.create_missing_questions import create_missing_questions
from mbs_results.staging.data_cleaning import (
    enforce_datatypes,
    filter_out_questions,
    run_live_or_frozen,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.utilities.utils import read_colon_separated_file


def create_form_type_spp_column(
    contributors: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    maps IDBR form types to SPP and creates column named "form_type_spp"

    Parameters
    ----------
    contributors : pd.DataFrame
        contributors dataframe from JSON snapshot
    config : dict
        main pipeline config containing "idbr_to_spp" mapping

    Returns
    -------
    pd.DataFrame
        contributors dataframe with "form_type_spp" column added
    """
    idbr_to_spp_mapping = config["idbr_to_spp"]
    contributors[config["form_id_spp"]] = contributors[config["form_id_idbr"]].map(
        idbr_to_spp_mapping
    )
    return contributors


def create_mapper() -> dict:
    """
    placeholder function to create question and form mapping dict

    Returns
    -------
    dict
        dictionary containing question numbers and form id.
    """
    mapper = {
        9: [40, 49],
        10: [110],
        11: [40, 49, 90],
        12: [40],
        13: [46, 47],
        14: [42, 43],
        15: [40],
        16: [40],
    }
    warnings.warn("create_mapper needs to be fully defined and moved to config")
    return mapper


def read_and_combine_colon_sep_files(
    folder_path: str, column_names: list, config: dict
) -> pd.DataFrame:
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
    df = pd.concat(
        [
            read_colon_separated_file(f, column_names, period=config["period"])
            for f in glob.glob(folder_path)
        ],
        ignore_index=True,
    )
    return df


def stage_dataframe(config: dict) -> pd.DataFrame:
    """
    wrapper function to stage and pre process the dataframe, ready to be passed onto the
    imputation wrapper (impute)

    Parameters
    ----------
    config : dict
        config containing paths and column names and file paths

    Returns
    -------
    pd.DataFrame
        Combined dataframe containing response and contributor data. Missing questions
        have been created, data types enforced. NI cell number have been converted
        to uk.
    """

    print("Staging started")
    period = config["period"]
    reference = config["reference"]

    contributors, responses = get_dfs_from_spp(
        config["folder_path"] + config["mbs_file_name"],
        config["platform"],
        config["bucket"],
    )

    # Filter columns and set data types
    contributors = contributors[config["contributors_keep_cols"]]
    contributors = enforce_datatypes(
        contributors, keep_columns=config["contributors_keep_cols"], **config
    )

    responses = responses[config["responses_keep_cols"]]
    responses = enforce_datatypes(
        responses, keep_columns=config["responses_keep_cols"], **config
    )

    finalsel = read_and_combine_colon_sep_files(
        config["sample_path"], config["sample_column_names"], config
    )

    finalsel = finalsel[config["finalsel_keep_cols"]]
    finalsel = enforce_datatypes(
        finalsel, keep_columns=config["finalsel_keep_cols"], **config
    )

    # Filter contributors files here to temp fix this overlap

    contributors = pd.merge(
        left=contributors,
        right=finalsel,
        on=[period, reference],
        suffixes=["_spp", "_finalsel"],
        how="outer",
    )
    # Should raise warning for left only or right only joins (missing in other df)
    #

    contributors = create_form_type_spp_column(contributors, config)
    mapper = create_mapper()  # Needs to be defined

    responses_with_missing = create_missing_questions(
        contributors_df=contributors,
        responses_df=responses,
        reference=reference,
        period=period,
        formid=config["form_id_spp"],
        question_no=config["question_no"],
        mapper=mapper,
    )

    df = responses_with_missing.drop(columns=config["form_id_spp"]).merge(
        contributors, on=[reference, period], suffixes=["_res", "_con"], how="left"
    )

    df = append_back_data(df, config)

    df = filter_out_questions(
        df=df,
        column=config["question_no"],
        questions_to_filter=config["filter_out_questions"],
        save_full_path=config["output_path"]
        + config["mbs_file_name"]
        + "filter_out_questions.csv",
    )

    warnings.warn("add live or frozen after fixing error marker column in config")
    df = run_live_or_frozen(
        df,
        config["target"],
        error_marker=config["errormarker"],
        state=config["state"],
        error_values=[201],
    )

    print("Staging Completed")

    return df
