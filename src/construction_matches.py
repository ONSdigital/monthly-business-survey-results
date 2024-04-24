import pandas as pd

def flag_construction_matches(dataframe, auxiliary_variable, period):
    """
    Find matched records based on the contributor's auxiliary information and
    period of time

    Parameters
    ----------
    dataframe : pandas.DataFrame
    auxiliary_variable : string
        name of column containing auxiliary information
    period : string
        name of column containing time period

    Returns
    -------
    pandas.DataFrame
        dataframe with flag column
    """

    return dataframe