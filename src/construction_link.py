import numpy as np


def calculate_construction_link(
    dataframe,
    target,
    auxiliary,
    match_flag,
    strata,
    period,
):
    """_summary_

    Parameters
    ----------
    dataframe : pandas.DataFrame
        _description_
    target : str
        name of column in dataframe containing target variable
    auxiliary : str
        name of column in dataframe containing auxiliary variable
    match_flag : str
        name of column in dataframe containing flag for valid construction matches
    strata : str
        name of column in dataframe containing strata variable
    period : str
        name of column in dataframe containing period variable
    """
    group_sums = (
        dataframe[dataframe[match_flag]]  # select valid matches
        .groupby([strata, period])
        .sum()
        .reset_index()
    )

    group_sums[auxiliary] = group_sums[auxiliary].replace(0, np.nan)

    group_sums["construction_link"] = group_sums[target] / group_sums[auxiliary]

    construction_link_df = group_sums[[strata, period, "construction_link"]]

    dataframe = dataframe.merge(construction_link_df, how="left", on=[strata, period])

    dataframe = dataframe.fillna({"construction_link": 1.0})

    return dataframe
