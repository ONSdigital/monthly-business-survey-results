import logging
import warnings

import numpy as np
import pandas as pd

from mbs_results.utilities.utils import (
    check_above_one,
    check_duplicates,
    check_non_negative,
    check_unique_per_cell_period,
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


def validate_manual_constructions(
    responses: pd.DataFrame, manual_constructions: pd.DataFrame, merge_on_cols: list
):
    """
    Check to ensure there isn't a return (and therefore not in the responses
    table in the spnashot) and MC for same reference-period-question combination.

    Parameters
    ----------
    responses: pd.DataFrame
        the responses dataframe
    manual_constructions: pd.DataFrame
        the manual constructions input read in as a dataframe
    merge_on_cols : list
        list of column to check for common observations in input dataframes.

    Raises
    ------
    ValueError
        ValueError if any combinations of period and reference appear in the manual
        constructions input and  in the responses dataframe.
    """

    inner_joined = pd.merge(
        left=responses,
        right=manual_constructions,
        how="inner",
        on=merge_on_cols,
        suffixes=("_from_responses", "_from_manual_constructions"),
    )

    if not inner_joined.empty:

        inner_joined.to_csv("valueError_validate_manual_constructions.csv", index=False)

        raise ValueError(
            """There are cases where a value exists in responses
and in manual constructions, this is not possible, saving the results to
the current working directory in a dataframe called
`valueError_validate_manual_constructions.csv`"""
        )


def validate_staging(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post staging")


def validate_imputation(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post imputation")


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

    if config["group"] != config["strata"]:
        validate_combined_ratio_estimation(
            population_frame=df,
            config=config,
        )


def validate_combined_ratio_estimation(
    population_frame: pd.DataFrame,
    config: dict,
) -> None:
    """
    Run all validation checks on combined ratio estimation.

    This function performs the following checks:

    1. Checks for duplicate rows based on 'period', 'reference', and 'question'.
    2. Verifies that weight columns exist and contain no missing values.
    3. Ensures unique design weights and calibration factors per cell/period.
    4. Checks that 'auxilary' and 'calibration_factor' contain non-negative values.
    5. Checks that 'design_weight' is above one.

    Parameters
    ----------
    population_frame : pandas.DataFrame
        A data frame with estimation weights.
    weight_columns : list of str
        A list of weight columns to check for missing values.
    unique_checks : list of str
        A list of columns to check for unique values per cell/period.
    non_negative_checks : dict of str to str
        A list of columns to check for non-negative values
    design_weight : str
        The name of the design weight column.

    Raises
    ------
    `ValueError`
    """
    check_duplicates(
        population_frame, [config["period"], config["reference"], config["question_no"]]
    )

    for column in [config["design_weight"], config["calibration_factor"]]:
        check_unique_per_cell_period(
            population_frame, config["cell_number"], config["period"], column
        )

    for column in [config["calibration_factor"], config["auxiliary"]]:
        check_non_negative(population_frame, column)

    check_above_one(population_frame, config["design_weight"])


def validate_outlier_detection(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post outliering")

    design_weight = config["design_weight"]

    invalid_rows = df[(df[design_weight] == 1) & (df["outlier_weight"] != 1)]

    if not invalid_rows.empty:
        references = invalid_rows["reference"].tolist()

        # Log the error with the references included
        logger.error(
            "There are instances where the design weight = 1 and outlier_weight != 1."
            f"References: {references}"
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
