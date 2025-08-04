from math import isclose
from typing import List

import numpy as np
import pandas as pd


def validate_estimation(
    population_frame: pd.DataFrame,
    period: str,
    reference: str,
    cell_number: str,
    design_weight: str,
    calibration_factor: str,
    calibration_group: str,
    group: str,
    strata: str,
    auxiliary: str,
    region: str,
    sampled: str,
    non_sampled_strata: List[str],
    **config,
) -> None:
    """
    Validation for the estimation, including:
    - no missing values in weight columns
    - weighted sum equal to unweighted sum

    Parameters
    ----------
    population_frame : pd.DataFrame
        The population frame containing data for estimation, including weights and
        auxiliary variables.
    period : str
        The time period for which the estimation is being validated.
    reference : str
        A reference identifier for the estimation process.
    cell_number : str
        The column name in the DataFrame representing cell numbers.
    design_weight : str
        The column name in the DataFrame representing the design weights.
    calibration_factor : str
        The column name in the DataFrame representing the calibration factors.
    calibration_group : str
        The column name in the DataFrame representing calibration groups.
    group: str
        The column name in the DataFrame representing group information.
    strata : str
        The column name in the DataFrame representing strata information.
    auxiliary : str
        The column name in the DataFrame representing auxiliary variables used in
        estimation.
    region : str
        The column name in the DataFrame representing regional information.
    sampled : str
        The column name in the DataFrame indicating whether a unit was sampled.
    non_sampled_strata : List[str]
        A list of strata that were not sampled in the estimation process.

    Raises
    ------
    `ValueError`
    """
    df = population_frame.copy()

    df = df[df[region] != "YY"]
    # YY corresponds to Northern Ireland, whose non-sampled strata are dealt
    # with separately in pipeline

    df = df[~df[strata].isin(non_sampled_strata)]

    for column in [design_weight, calibration_factor]:
        if df[column].isna().any():
            raise ValueError(
                f"""
Weight column should have no missing values following estimation:
missing values found in column {column}
                """
            )

    unweighted_sum = df[auxiliary].sum()

    sampled_df = df[df[sampled] == 1]

    weighted_sum = (
        sampled_df[auxiliary]
        * sampled_df[design_weight]
        * sampled_df[calibration_factor]
    ).sum()

    if not (isclose(weighted_sum, unweighted_sum, rel_tol=1e-09)):
        raise ValueError(
            f"""
Sum of auxiliary variable multiplied by design weight and calibration factor
should be equal to sum of auxiliary variable.
Auxiliary: {auxiliary}
Unweighted sum: {unweighted_sum}
Weighted sum: {weighted_sum}
            """
        )

    required_columns = {
        period,
        cell_number,
        sampled,
        auxiliary,
        calibration_group,
        reference,
    }

    weight_columns = ["weight", "g_weight"]

    unique_checks = [design_weight, calibration_factor]

    non_negative_checks = {calibration_factor, auxiliary}

    if group != strata:
        validate_combined_ratio_estimation(
            df,
            required_columns=required_columns,
            weight_columns=weight_columns,
            unique_checks=unique_checks,
            non_negative_checks=non_negative_checks,
        )


def check_duplicates(df, columns):
    """Check for duplicate rows based on specified columns."""
    if df.duplicated(subset=columns).any():
        raise ValueError(f"Duplicate rows found based on columns: {columns}")


def check_missing_values(df, column):
    """Check for missing values in a specific column."""
    if column not in df.columns:
        raise ValueError(f"Missing required column: {column}")
    if df[column].isnull().any():
        raise ValueError(f"Column {column} contains missing values.")


def check_missing_argument(argument, argument_name):
    """Check if a required argument is missing."""
    if argument is None:
        raise ValueError(f"Missing required argument: {argument_name}")


def check_input_types(df, expected_types):
    """Check types of input variables."""
    for column, expected_type in expected_types.items():
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}")
        if not np.issubdtype(df[column].dtype, expected_type):
            raise TypeError(f"Column {column} is not of type {expected_type}")


def check_population_sample(df, population_column, sample_column):
    """Check if population = sample, then a and g should be 1."""
    if (df[population_column] == df[sample_column]).all():
        if not (df["a"] == 1).all():
            raise ValueError("If population = sample, all refs must have a = 1")
        if not (df["g"] == 1).all():
            raise ValueError("If population = sample, all refs must have g = 1")


def check_weights_exist(df, weight_columns):
    """Check dataset contains a weight and g weight for all rows."""
    for column in weight_columns:
        if column not in df.columns:
            raise ValueError(f"Missing required weight column: {column}")
        if df[column].isnull().any():
            raise ValueError(f"Missing weights in column: {column}")


def check_unique_per_cell_period(df, cell_column, period_column, weight_column):
    """Check that for each cell/period there is only one unique design weight."""
    if (
        cell_column not in df.columns
        or period_column not in df.columns
        or weight_column not in df.columns
    ):
        raise ValueError("Missing required columns for uniqueness check.")
    if df.groupby([cell_column, period_column])[weight_column].nunique().max() > 1:
        raise ValueError(
            f"Multiple unique values found for {weight_column} in each cell/period"
        )


def check_non_negative(df, column):
    """Check that all values in a column are non-negative."""
    if column not in df.columns:
        raise ValueError(f"Missing required column: {column}")
    if (df[column] < 0).any():
        raise ValueError(f"Column {column} contains negative values.")


def validate_combined_ratio_estimation(
    population_frame: pd.DataFrame,
    required_columns: dict[str, str],
    weight_columns: list[str],
    unique_checks: list[str],
    non_negative_checks: dict[str, str],
) -> None:
    """
    Run all validation checks on combined ratio estimation.

    This function performs the following checks:

    1. Checks for duplicate rows based on 'period', 'reference', and 'question'.
    2. Ensures required columns are present and contain no missing values.
    3. Verifies that weight columns exist and contain no missing values.
    4. Ensures unique design weights and calibration factors per cell/period.
    5. Checks that specific columns contain non-negative values.

    Parameters
    ----------
    population_frame : pandas.DataFrame
        A data frame with estimation weights.
    required_columns : dict of str to str
        A dictionary of required columns
    weight_columns : list of str
        A list of weight columns to check for missing values.
    unique_checks : list of str
        A list of columns to check for unique values per cell/period.
    non_negative_checks : dict of str to str
        A dictionary of columns to check for non-negative values

    Raises
    ------
    `ValueError`
    """
    check_duplicates(population_frame, ["period", "reference", "question"])

    for column in required_columns:
        check_missing_values(population_frame, column)

    check_weights_exist(population_frame, weight_columns)

    for column in unique_checks:
        check_unique_per_cell_period(population_frame, "cell", "period", column)

    for column in non_negative_checks:
        check_non_negative(population_frame, column)
