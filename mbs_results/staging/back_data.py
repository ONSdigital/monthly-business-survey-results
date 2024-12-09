import pandas as pd


def have_same_columns(*dfs: pd.DataFrame) -> bool:
    """Checks if input dataframes have same columns

    Parameters
    ----------
    *dfs : pd.DataFrame
        Dataframes to check columns.

    Raises
    ------
    ValueError
        If number of input dataframes not enough for comparison m .

    Returns
    -------
    bool
        True if all column are equal.

    """

    if len(dfs) < 2:
        raise ValueError("At least 2 dataframes required for comparison.")

    reference_columns = dfs[0].columns

    return all(df.columns.equals(reference_columns) for df in dfs[1:])


def is_next_month(date: pd.Datetime, date_to_check: pd.Datetime) -> bool:
    """Given 2 dates check if later is next month.

    Parameters
    ----------
    date : pd.Datetime
        Date to check.
    date_to_check : pd.Datetime
        Date to check against.

    Returns
    -------
    bool
        True if date_to_check is next month.

    """
    return date_to_check == date + pd.DateOffset(months=1)


def append_back_data(
    df: pd.DataFrame, back_data: pd.DataFrame, period: str
) -> pd.DataFrame:
    """
    Appends back data with period 0 to main dataframe. Period 0 is needed
    to apply forward imputation in the first period of df.

    EXPECTS datetime dtype for period, this is enforced during data reading
    for both back and df.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    back_data : pd.DataFrame
        Dataframe with back containing period 0.
    period : str
        Column name with dates..

    Raises
    ------
    ValueError
        If first date is not next month of period 0 and dataframes do not have
        same columns.

    Returns
    -------
    pd.DataFrame
        Original dataframe with period 0 back data.
    """

    period_0 = df[period].unique()[0]

    min_period = min(df[period])

    if not is_next_month(period_0, min_period):

        raise ValueError(
            """{} (first date of dataframe)is not next month of {}
                (period 0 from back data)""".format(
                min_period, period_0
            )
        )
        if not have_same_columns(back_data, df):

            raise ValueError("Dataframes don't have same columns.")

    return pd.concat([back_data, df])
