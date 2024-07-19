import pandas as pd


def aggregate_aweights_by_class(
    dataframe: pd.DataFrame,
    imp_class: str,
    period: str,
    a_weight: str,
    current_period: int,
) -> pd.DataFrame:
    """
    Returning imp_class and design weights (a_weights) for current period
    based on reference dataframe.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe with imp_class and a_weights
    imp_class : str
        name of column in dataframe containing imp_class variable
    period : str
        name of column in dataframe containing period variable
    a_weight : str
        name of column in dataframe containing a_weight variable
    current_period : int
        Current period to take a_weights for, in the format yyyymm

    Returns
    -------
    pd.DataFrame
        dataframe with aggregated imp_class and a_weights values for current period.

    """
    current_df = dataframe[dataframe[period] == current_period]

    aggregate_df = current_df[[period, imp_class, a_weight]].drop_duplicates()

    return aggregate_df.reset_index(drop=True)
