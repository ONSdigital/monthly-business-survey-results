import pandas as pd


def calculate_a_weight(
    dataframe: pd.DataFrame, period: str, strata: str, sampled: str
) -> pd.DataFrame:
    """
    Add column to dataframe containing a weights based on sampled flag

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
        dataframe with new column `a_weight`

    Notes
    -----
    Assumes all strata are sampled - is this a valid assumption?
    #TODO: Add link to specification
    """
    population_counts = dataframe.groupby([period, strata]).size()

    sample = dataframe[dataframe[sampled] == 1]
    sample_counts = sample.groupby([period, strata]).size()

    a_weights = population_counts / sample_counts

    a_weights.name = "a_weight"
    a_weights = a_weights.reset_index()

    dataframe = dataframe.merge(a_weights, how="left", on=[period, strata])

    return dataframe
