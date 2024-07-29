import pandas as pd


def calculate_design_weight(
    population_frame: pd.DataFrame,
    period: str,
    strata: str,
    sampled: str,
    **config,
) -> pd.DataFrame:
    """
    Add column to dataframe containing design weights based on sampled flag

    Parameters
    ----------
    population_frame : pd.DataFrame
        data to use to calculate weights
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
    population_counts = population_frame.groupby([period, strata]).size()

    sample = population_frame[population_frame[sampled] == 1]
    sample_counts = sample.groupby([period, strata]).size()

    design_weights = population_counts / sample_counts

    design_weights.name = "design_weight"
    design_weights = design_weights.reset_index()

    population_frame = population_frame.merge(
        design_weights, how="left", on=[period, strata]
    )

    return population_frame


def calculate_calibration_factor(
    population_frame: pd.DataFrame,
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
    population_frame : pd.DataFrame
        data to use to calculate weights
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

    population_sums = population_frame.groupby([period, group])[auxiliary].sum()

    # copy to avoid SettingWithCopy warning
    # (not required with later versions of pandas)
    sample = population_frame.copy()[population_frame[sampled] == 1]
    sample["weighted_auxiliary"] = sample[auxiliary] * sample[design_weight]
    weighted_sample_sums = sample.groupby([period, group])["weighted_auxiliary"].sum()

    calibration_factor = population_sums / weighted_sample_sums

    calibration_factor.name = "calibration_factor"
    calibration_factor = calibration_factor.reset_index()

    population_frame = population_frame.merge(
        calibration_factor, how="left", on=[period, group]
    )

    return population_frame
