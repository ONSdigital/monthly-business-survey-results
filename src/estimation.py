from typing import Optional

import pandas as pd


def calculate_a_weight(
    dataframe: pd.DataFrame,
    period: str,
    group: str,
    sampled: str,
    a_weight: Optional[str] = "a_weight",
) -> pd.DataFrame:
    """
    Add column to dataframe containing a weights based on sampled flag

    Parameters
    ----------
    dataframe : pd.DataFrame
        data to be estimated
    period : str
        name of column in dataframe containing period variable
    group : str
        name of column in dataframe containing group variable
        this should usually be the strata variable
    sampled : str
        name of column in dataframe containing sample flag with values 0 or 1
    a_weight : str
        name to be given to new column containing a_weights
        defaults to `a_weights`

    Returns
    -------
    pd.DataFrame
        dataframe with new column containing a_weights

    Notes
    -----
    Assumes all strata are sampled - is this a valid assumption?
    #TODO: Add link to specification
    """
    population_counts = dataframe.groupby([period, group]).size()

    sample = dataframe[dataframe[sampled] == 1]
    sample_counts = sample.groupby([period, group]).size()

    a_weights = population_counts / sample_counts

    a_weights.name = a_weight
    a_weights = a_weights.reset_index()

    dataframe = dataframe.merge(a_weights, how="left", on=[period, group])

    return dataframe


def calculate_calibration_factor(
    dataframe: pd.DataFrame,
    period: str,
    group: str,
    sampled: str,
    auxiliary: str,
) -> pd.DataFrame:
    """
     Add column to dataframe to calculate calibration factor

     Parameters
     ----------
     dataframe : pd.DataFrame
         data to be weighted
     period : str
         name of column in dataframe containing period variable
    group: str
         name of column in dataframe containing group level data
         for separate ratio, use strata variable
         for combined ratio, use calibration group level variable
     sampled : str
         name of column in dataframe containing sample flag
     auxiliary : str
         name of column in dataframe containing auxiliary variable
     design_weight: str
         name of column in dataframe containing design weight

     Returns
     -------
     pd.DataFrame
         dataframe with new column `calibration_factor`
    """
    # design weights used in calibration factor calculation should be based on
    # group (not necessarily strata) and should not overwrite existing weights
    dataframe_copy = dataframe.copy()
    dataframe_copy = calculate_a_weight(
        dataframe_copy, period, group, sampled, "group_design_weight"
    )

    population_sums = dataframe_copy.groupby([period, group])[auxiliary].sum()

    # copy again to avoid SettingWithCopy warning
    # (not required with later versions of pandas)
    sample = dataframe_copy.copy()[dataframe_copy[sampled] == 1]
    sample["weighted_auxiliary"] = sample[auxiliary] * sample["group_design_weight"]
    weighted_sample_sums = sample.groupby([period, group])["weighted_auxiliary"].sum()

    calibration_factor = population_sums / weighted_sample_sums

    calibration_factor.name = "calibration_factor"
    calibration_factor = calibration_factor.reset_index()

    dataframe = dataframe.merge(calibration_factor, how="left", on=[period, group])

    return dataframe
