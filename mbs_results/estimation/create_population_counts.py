import pandas as pd


def calculate_turnover_sum_count(
    df: pd.DataFrame, period: str, strata: str, colname: str, **config
) -> pd.DataFrame:
    """
    Calculates turnover sum and count and returns an aggregated dataframe
    with the given column name prefixed to the sum and count columns

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe containing frotover. Groups by period and strata
    period : str
        period column name
    strata : str
        strate column name
    colname : str
        column name to prefix to the sum and count columns

    Returns
    -------
    pd.DataFrame
        A grouped dataframe with the sum and count columns prefixed with colname
    """

    df_pop_count = (
        df.groupby([period, strata])
        .agg(summing=("frotover", "sum"), count=("reference", "size"))
        .reset_index()
    )

    df_pop_count.rename(
        columns={"summing": f"{colname}_turnover_sum", "count": f"{colname}_count"},
        inplace=True,
    )

    return df_pop_count


def create_population_count_output(
    df: pd.DataFrame, period, strata, **config
) -> pd.DataFrame:
    """
    creates the population count output

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe frotover and sampled. Groups by period and strata
    period : str
        period column name
    strata : str
        strata column name

    Returns
    -------
    pd.DataFrame
        A grouped dataframe with the sum and count columns prefixed with colname.
        Contains both population and sampled sum and counts for output.
    """

    df_1 = calculate_turnover_sum_count(
        df, period, strata, colname="population", **config
    )

    df_2 = calculate_turnover_sum_count(
        df.loc[df["sampled"]], period, strata, colname="sample", **config
    )
    combined = pd.merge(df_1, df_2, on=[period, strata])
    return combined
