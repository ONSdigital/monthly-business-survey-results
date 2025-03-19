import glob
import warnings

import pandas as pd

from mbs_results.staging.back_data import append_back_data
from mbs_results.staging.create_missing_questions import (
    create_mapper,
    create_missing_questions,
)
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    enforce_datatypes,
    filter_out_questions,
    run_live_or_frozen,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.utilities.utils import (
    convert_column_to_datetime,
    read_colon_separated_file,
)


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

    snapshot_name = config["mbs_file_name"].split(".")[0]

    df = filter_out_questions(
        df=df,
        column=config["question_no"],
        questions_to_filter=config["filter_out_questions"],
        save_full_path=config["output_path"]
        + snapshot_name
        + "_filter_out_questions.csv",
    )

    df = drop_derived_questions(
        df,
        config["question_no"],
        config["form_id_spp"],
    )

    warnings.warn("add live or frozen after fixing error marker column in config")
    df = run_live_or_frozen(
        df,
        config["target"],
        status=config["status"],
        state=config["state"],
        error_values=[201],
    )

    df[config["auxiliary_converted"]] = df[config["auxiliary"]].copy()
    df = convert_annual_thousands(df, config["auxiliary_converted"])

    print("Staging Completed")
    return df


def drop_derived_questions(
    df: pd.DataFrame, question_no: str, form_type_spp: str
) -> pd.DataFrame:
    """
    drops rows containing derived questions based on spp form type

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe with spp form type column
    question_no : str
        column name for question number
    form_type_spp : str
        column name for spp form type
    Returns
    -------
    pd.DataFrame
        _description_
    """

    question_dict = {
        13: 40,
        14: 40,
        15: 46,
        16: 42,
    }

    for formid, question_number in question_dict.items():
        filtered_df = df.loc[
            (df[question_no] == question_number) & (df[form_type_spp] == formid)
        ]
        if not filtered_df.empty:
            warnings.warn(
                f"Derived question dataframe {question_number} for "
                + f"formid {formid} was found in the staged dataframe. "
                + "Dropping"
            )
        df = df.drop(
            df[
                (df[question_no] == question_number) & (df[form_type_spp] == formid)
            ].index
        )
    return df


def start_of_period_staging(
    imputation_output: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    Stages the imputation output at the start of a period based on the provided
    configuration.

    Parameters
    ----------
    imputation_output : pd.DataFrame
        The DataFrame containing the imputation output.
    config : dict
        A dictionary containing configuration parameters. Expected keys include:
        - "current_period": The current period to filter the imputation output.
        - "finalsel_keep_cols": List of columns to keep in the final selection.
        - "sample_path": Path to the sample files.
        - "sample_column_names": Column names for the sample files.
        - "period_selected": The period selected for final selection.
        - "idbr_to_spp": Mapping from IDBR to SPP.
        - "form_id_spp": Column name for form ID SPP.
        - "form_id_idbr": Column name for form ID IDBR.
        - "reference": Reference column name.
        - "period": Period column name.
        - "question_no": Question number column name.

    Returns
    -------
    pd.DataFrame
        The staged imputation output DataFrame with missing questions created and
        merged with final selection.
    """
    if config["current_period"] in imputation_output["period"].unique():

        imputation_output = imputation_output.loc[
            imputation_output["period"] == config["current_period"]
        ]

        imputation_output["period"] = convert_column_to_datetime(
            imputation_output["period"]
        ) + pd.DateOffset(months=1)

        drop_cols = [
            col
            for col in config["finalsel_keep_cols"]
            if col not in ["reference", "period"]
        ]
        imputation_output.drop(columns=drop_cols, inplace=True, errors="ignore")

        finalsel = read_and_combine_colon_sep_files(
            config["sample_path"], config["sample_column_names"], config
        )

        finalsel = finalsel.loc[finalsel["period"] == config["period_selected"]]

        finalsel = finalsel[config["finalsel_keep_cols"]]
        finalsel = enforce_datatypes(
            finalsel, keep_columns=config["finalsel_keep_cols"], **config
        )

        mapper = create_mapper()
        idbr_to_spp_mapping = config["idbr_to_spp"]
        finalsel[config["form_id_spp"]] = (
            finalsel[config["form_id_idbr"]].astype(str).map(idbr_to_spp_mapping)
        )

        imputation_output_with_missing = create_missing_questions(
            contributors_df=finalsel,
            responses_df=imputation_output,
            reference=config["reference"],
            period=config["period"],
            formid=config["form_id_spp"],
            question_no=config["question_no"],
            mapper=mapper,
        )

        imputation_output_with_missing = pd.merge(
            left=imputation_output_with_missing,
            right=finalsel,
            on=[config["period"], config["reference"]],
            suffixes=["_imputation_output", "_finalsel"],
            how="right",
        )

        imputation_output_with_missing["period"] = (
            imputation_output_with_missing["period"].dt.strftime("%Y%m").astype(int)
        )

    return imputation_output_with_missing
