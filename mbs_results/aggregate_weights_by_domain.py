import pandas as pd


def aggregate_weights_by_domain(
    dataframe: pd.DataFrame,
    period: str,
    domain: str,
    a_weight: str,
    o_weight: str,
    g_weight: str,
    previous_period: int,
) -> pd.DataFrame:
    """
    Returning design weights (a_weights), outliering weights (o_weights)
    and calibration factors (g_weights) grouped by domain for previous
    period based on reference dataframe.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe with domain, a_weights, o_weights, and g_weights
    period : str
        name of column in dataframe containing period variable
    domain : str
        name of column in dataframe containing domain variable
    a_weight : str
        name of column in dataframe containing a_weight variable
    o_weight : str
        name of column in dataframe containing o_weight variable
    g_weight : str
        name of column in dataframe containing g_weight variable
    previous_period : int
        Previous period to take domain, a_weight, o_weight, and g_weight for,
        in the format yyyymm

    Returns
    -------
    pd.DataFrame
        dataframe with aggregated domain, a_weights, o_weights, and g_weights
        values for current period.

    """
    previous_df = dataframe[dataframe[period] == previous_period]

    aggregate_df = previous_df[[domain, a_weight, o_weight, g_weight]].drop_duplicates()

    return aggregate_df.reset_index(drop=True)
