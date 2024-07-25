import pandas as pd


def calculate_design_weight(
    dataframe: pd.DataFrame,
    period: str,
    strata: str,
    sampled: str,
    **config,
) -> pd.DataFrame:
    """
    Add column to dataframe containing design weights based on sampled flag

    Parameters
    ----------
    dataframe : pd.DataFrame
        data to be estimated
    period : str
        name of column in dataframe containing period variable
    strata : str
        name of column in dataframe containing strata variable
    sampled : str
        name of column in dataframe containing sample flag with values 0 or 1

    Returns
    -------
    pd.DataFrame
        dataframe with new column design_weight

    Notes
    -----
    #TODO: Add link to specification once added to repository
    """
    population_counts = dataframe.groupby([period, strata]).size()

    sample = dataframe[dataframe[sampled] == 1]
    sample_counts = sample.groupby([period, strata]).size()

    design_weights = population_counts / sample_counts

    design_weights.name = "design_weight"
    design_weights = design_weights.reset_index()

    dataframe = dataframe.merge(design_weights, how="left", on=[period, strata])

    return dataframe


def calculate_calibration_factor(
    dataframe: pd.DataFrame,
    period: str,
    group: str,
    sampled: str,
    auxiliary: str,
    design_weight: str,
    **config,
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

    population_sums = dataframe.groupby([period, group])[auxiliary].sum()

    # copy to avoid SettingWithCopy warning
    # (not required with later versions of pandas)
    sample = dataframe.copy()[dataframe[sampled] == 1]
    sample["weighted_auxiliary"] = sample[auxiliary] * sample[design_weight]
    weighted_sample_sums = sample.groupby([period, group])["weighted_auxiliary"].sum()

    calibration_factor = population_sums / weighted_sample_sums

    calibration_factor.name = "calibration_factor"
    calibration_factor = calibration_factor.reset_index()

    dataframe = dataframe.merge(calibration_factor, how="left", on=[period, group])

    return dataframe
