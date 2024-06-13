def filter_responses(df, reference, period, last_update):
    """
    Filter the responses data to return only the most recently updated
    row for each reference and period combination

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing responses data from SPP

    reference : str
        Column name containing business reference id.
    period : str
        Column name for survey period.
    last_update : str
        Column name containing auxiliary data.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame of one reference period combination
    """
    return df.sort_values(last_update).groupby([reference, period]).tail(1)
