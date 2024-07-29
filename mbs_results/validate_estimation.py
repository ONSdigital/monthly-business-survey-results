from math import isclose
from typing import List

import pandas as pd


def validate_estimation(
    population_frame: pd.DataFrame,
    design_weight: str,
    calibration_factor: str,
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
        population frame with estimation weights
    design_weight : str
        name of column containing design weight
    calibration_factor : str
        name of column containing calibration factor
    strata : str
        name of column containing strata
    auxiliary : str
        name of column containing auxiliary variable
    region : str
        name of column containing region variable
    sampled : str
        name of column containing sampled indicator
    non_sampled_strata : List[str]
        list of stata deliberately not sampled

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

    if not (isclose(weighted_sum, unweighted_sum)):
        raise ValueError(
            f"""
Sum of auxiliary variable multiplied by design weight and calibration factor
should be equal to sum of auxiliary variable.
Auxiliary: {auxiliary}
Unweighted sum: {unweighted_sum}
Weighted sum: {weighted_sum}
            """
        )
