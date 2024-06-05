import pandas as pd


def calculate_a_weight(
    dataframe: pd.DataFrame, strata: str, sampled: str
) -> pd.DataFrame:
    """
    Add column to dataframe containing a weights based on sampled flag

    Parameters
    ----------
    dataframe : pd.DataFrame
        data to be estimated
    strata : str
        name of column in dataframe containing strata variable
    sampled : str
        name of column in dataframe containing sample flag (0 or 1)

    Returns
    -------
    pd.DataFrame
        dataframe with new column `a_weight`

    Notes
    -----
    Assumes all strata are sampled - is this a valid assumption?
    <link to specification>
    """
    # also groupby period? (not mentioned in spec and only one period in worked example)
    population_counts = dataframe.groupby(strata).size()

    sample = dataframe[dataframe[sampled] == 1]
    sample_counts = sample.groupby(strata).size()

    a_weights = population_counts / sample_counts

    a_weights.name = "a_weight"
    a_weights = a_weights.reset_index()

    dataframe = dataframe.merge(a_weights, how="left", on=strata)

    return dataframe
