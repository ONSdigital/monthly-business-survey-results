import pandas as pd
import numpy as np

def flag_construction_matches(dataframe, auxiliary_variable, period):
    """
    Add flag to indicate whether there exists at least one other record with
    the same auxiliary information in that period of time

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

    group_size = dataframe.groupby([auxiliary_variable, period]).size()

    group_size.name = "size"

    group_size = group_size.reset_index()

    dataframe = dataframe.merge(group_size, how="left", on=[auxiliary_variable, period])

    dataframe["flag_construction_matches"] = np.where(
        (dataframe["size"]>1),
        True,
        False
    )

    dataframe = dataframe.drop("size", axis=1)

    return dataframe