import pandas as pd

def flag_construction_matches(dataframe, target, period, auxiliary):
    """
    Add flag to indicate whether the record has non-null target, period and
    auxiliary variables, and is therefore valid to use when calculating
    construction links

    Parameters
    ----------
    dataframe : pandas.DataFrame
    target : string
        name of column containing the target variable
    period : string
        name of column containing time period
    auxiliary : string
        name of column containing auxiliary information

    Returns
    -------
    pandas.DataFrame
        dataframe with additional flag_construction_matches column
    """

    dataframe["flag_construction_matches"] = pd.notna(dataframe[[target, period, auxiliary]]).all(axis="columns")

    return dataframe
