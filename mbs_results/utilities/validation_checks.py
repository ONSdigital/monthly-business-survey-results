import logging
import os
import warnings

import numpy as np
import pandas as pd

from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.utils import (
    append_filter_out_questions,
    get_versioned_filename,
)

logger = logging.getLogger(__name__)


def validate_config(config):
    """
    Run validation checks on the main config dictionary

    Parameters
    ----------
    config: Dict
      main pipeline configuration

    """

    if colnames_clash(**config):
        raise ValueError(
            """Overlapping column names in responses_keep_cols and
            contributors_keep_cols (main config)."""
        )
    if period_and_reference_not_given(**config):
        raise ValueError(
            """Period and/or Reference is not given in responses_keep_cols
             and/or contributors_keep_cols (main config). """
        )


def colnames_clash(
    reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Check for overlapping columns between the input data to be kept,
    except for id variables

    Parameters
    ----------
    reference: Str
      the name of the reference column
    period: Str
      the name of the period column
    response_keep_cols: Str
      the names of the columns to keep from the responses data
    contributors_keep_cols: Str
        the names of the columns to keep from the contributors data
    **config: Dict
      main pipeline configuration. Can be used to input the entire config dictionary
    Returns
    -------
    bool
      Returns true if any column names are in both contributors and responses,
      excluding period and reference.
      False otherwise
    """

    return any(
        [
            column in contributors_keep_cols and column not in [reference, period]
            for column in responses_keep_cols
        ]
    )


def period_and_reference_not_given(
    reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Function checks that both reference and period columns are supplied in
    response_keep_cols and contributors_keep_cols.
    Returns True if period or reference is missing from either
    responses_keep_cols and contributors_keep_cols, False otherwise

    reference: Str
      the name of the reference column
    period: Str
      the name of the period column
    response_keep_cols: Str
      the names of the columns to keep from the responses data
    contributors_keep_cols: Str
        the names of the columns to keep from the contributors data
    **config: Dict
      main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    bool
        Returns True if response and period are not in both responses_keep_cols
        and contributors_keep_cols
        False otherwise
    """

    return any(
        [
            column
            for column in [reference, period]
            if (column not in responses_keep_cols)
            or (column not in contributors_keep_cols)
        ]
    )


def validate_indices(responses, contributors):
    """
    Check that all indices (reference x period) match
    across the responses and contributors dataframes.

    Raises an error if there are any mismatches.

    Parameters
    ----------
    responses: pd.DataFrame
      the responses dataset with index variables set
    contributors: pd.DataFrame
      the contributors dataset with index variables set

    """

    diff = set(responses.index.unique()) ^ set(contributors.index.unique())

    if len(diff) > 0:
        non_matches = "".join([f"reference: {i[0]}, period: {i[1]}\n" for i in diff])
        raise ValueError(
            f"""There are mismatched records between the responses and
            contributors datasets:\n {non_matches}"""
        )


def validate_config_datatype_input(
    responses_keep_cols, contributors_keep_cols, **config
):
    """
    function to validate config datatypes inputted into the config file

    Parameters
    ----------
    responses_keep_cols : dict
        dictionary containing columns to keep from responses and datatypes
    contributors_keep_cols : _type_
        dictionary containing columns to keep from contributors and datatypes

    Raises
    ------
    ValueError
        ValueError if the specified datatype is not in the accepted_type list
        ints and floats do not need to specify number of bits i.e. int32 etc.
    """
    joint_dict = {**responses_keep_cols, **contributors_keep_cols}
    accepted_types = ["str", "float", "int", "date", "bool", "category"]
    incorrect_datatype = [
        item for item in list(joint_dict) if joint_dict.get(item) not in accepted_types
    ]

    if incorrect_datatype:
        given_types = [joint_dict.get(key) for key in incorrect_datatype]
        raise ValueError(
            """Check the inputted datatype(s) for column(s) {}:{},
            only the following datatypes are accepted: {}""".format(
                incorrect_datatype, given_types, accepted_types
            )
        )


def validate_config_repeated_datatypes(
    responses_keep_cols, contributors_keep_cols, **config
):
    """
    Checking that repeated columns do not have conflicting data types

    Parameters
    ----------
    responses_keep_cols : dict
        dictionary containing columns to keep from responses and datatypes
    contributors_keep_cols : _type_
        dictionary containing columns to keep from contributors and datatypes

    Raises
    ------
    ValueError
        ValueError if any repeated column has a different data type in the contributors
        and responses data sets.
    """

    mismatched_types = [
        key
        for key in responses_keep_cols
        if (key in contributors_keep_cols)
        and (responses_keep_cols[key] != contributors_keep_cols[key])
    ]
    if mismatched_types:
        # Warning to catch if the same column name has different types
        raise ValueError(
            "Mismatched data types between two dictionaries in columns: {}".format(
                mismatched_types
            )
        )


def validate_manual_constructions(df, manual_constructions):
    """
    Checks that manual construction identifiers match those in the main dataset

    Parameters
    ----------
    df: pd.DataFrame
        the main dataframe after preprocessing
    manual_constructions: pd.DataFrame
        the manual constructions input read in as a dataframe

    Raises
    ------
    ValueError
        ValueError if any combinations of period and reference appear in the manual
        constructions input but not in the main dataframe
    """

    incorrect_ids = set(manual_constructions.index) - set(df.index)

    if len(incorrect_ids) > 1:
        string_ids = " ".join(
            [f"\nreference: {str(i[0])}, period: {str(i[1])}" for i in incorrect_ids]
        )

        raise ValueError(
            f"""There are reference and period combinations in the manual constructions
      with no match: {string_ids}"""
        )


def validate_staging(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post staging")


def validate_imputation(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post imputation")
    output_path = config["output_path"]

    imputation_filename = get_versioned_filename("imputation", config)

    write_csv_wrapper(
        df=df,
        save_path=output_path + imputation_filename,
        import_platform=config["platform"],
        bucket_name=config["bucket"],
        index=False,
    )


def validate_estimation(df: pd.DataFrame, config: dict):
    """
    Validates the estimation output.

    Parameters
    ----------
    df: pd.DataFrame
        the main dataframe after running the estimation part of the pipeline
    config: Dict
        The config dictionary.


    Raises
    ------
    ValueError
        ValueError if there are any null values in either the census or sampled columns.
    """
    output_path = config["output_path"]
    estimate_filename = get_versioned_filename("estimation_output", config)
    census_nas = df[config["census"]].isna().sum()
    if census_nas > 0:
        raise ValueError(
            f'There are {census_nas} NA(s) in the {config["census"]} column.'
        )

    sampled_nas = df[config["sampled"]].isna().sum()
    if sampled_nas > 0:
        raise ValueError(
            f'There are {sampled_nas} NA(s) in the {config["sampled"]} column.'
        )

    write_csv_wrapper(
        df,
        output_path + estimate_filename,
        config["platform"],
        config["bucket"],
        index=False,
    )


def validate_outlier_detection(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post outliering")
    output_path = config["output_path"]

    design_weight = config["design_weight"]

    invalid_rows = df[(df[design_weight] == 1) & (df["outlier_weight"] != 1)]

    if not invalid_rows.empty:
        references = invalid_rows["reference"].tolist()

        # Log the error with the references included
        logger.error(
            "There are instances where the design weight = 1 and outlier_weight != 1."
            f"References: {references}"
        )

    outlier_filename = get_versioned_filename("outlier_output", config)

    # Must be same as save_full_path argument of filter_out_questions() (in staging)
    # Path must be full path containing directory and file name
    # e.g. folder1/folder2/snapsot_filter_out_questions.csv

    snapshot_name = os.path.basename(config["snapshot_file_path"]).split(".")[0]

    filtered_questions_path = (
        config["output_path"] + snapshot_name + "_filter_out_questions.csv"
    )
    df = append_filter_out_questions(df, filtered_questions_path)
    write_csv_wrapper(
        df,
        output_path + outlier_filename,
        config["platform"],
        config["bucket"],
        index=False,
    )


def validate_manual_outlier_df(
    df: pd.DataFrame,
    reference: str,
    period: str,
    question_code: str,
) -> bool:
    """
    Function to perform a set of validation checks on the ingested
    manual outlier data that is used to overwrite post-winsorisation
    derived outliers
    """

    # Check required columns exist
    if set([reference, period, question_code, "manual_outlier_weight"]).issubset(
        df.columns
    ):

        # Force column order:
        df = df[[reference, period, question_code, "manual_outlier_weight"]]

        # Check if data types do not match
        if df.dtypes.to_dict() != {
            reference: np.int64,
            period: np.int64,
            question_code: np.int64,
            "manual_outlier_weight": np.float64,
        }:

            raise Exception("Manual outlier data is not of the correct type")

        # Check if reference, period and question code have missing
        if df[[reference, period, question_code]].isna().all().all():

            raise Exception("Manual outlier weights are not linked to reponse records")

        # Check if all manual_outlier_weight is > 1 or < 0
        if (df["manual_outlier_weight"].max() > 1.0) or (
            df["manual_outlier_weight"].min() < 0.0
        ):

            raise Exception("Manual outlier weights are invalid")

    else:
        raise Exception("Manual outlier data does not have the correct columns")

    return True
