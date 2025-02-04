import numpy as np  # noqa F401
import pandas as pd  # noqa F401


def flag_matched_pair(
    df,
    forward_or_backward,
    target,
    period,
    reference,
    strata,
    imputation_flag,
    time_difference=1,
    **kwargs,
):
    """
    function to flag matched pairs using the shift method

    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    forward_or_backward : str
        number of rows to shift up or down
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)
    imputation_flag : str
        column name containing imputation flag
    time_difference: int
        lookup distance for matched pairs
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.

    Returns
    -------
    _type_
        pd.DataFrame: the main dataframe with column added flagging
        forward matched pairs and predictive target variable data column
    """

    df = df.sort_values(by=[strata, reference, period])

    if forward_or_backward == "b":
        time_difference = -time_difference

    df["actual_response"] = df[target].copy()

    df.loc[
        df[df[imputation_flag].isin(["fir", "fic", "fimc", "bir", "c", "mc"])].index,
        "actual_response",
    ] = None

    df[f"{forward_or_backward}_match_{target}"] = (
        df.groupby([strata, reference])
        .shift(time_difference)["actual_response"]
        .notnull()
        .mul(df["actual_response"].notnull())
        .mul(
            df[period] - pd.DateOffset(months=time_difference)
            == df.shift(time_difference)[period]
        )
    )

    df.drop("actual_response", axis=1, inplace=True)
    return df


def count_matches(df, flag, period, strata, **kwargs):
    """
    function to flag matched pairs using the shift method


    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data with imputation flags
    flag : str/list
        the imputation flag column/s. Single string if one column, list of
        strings for multiple columns.
    period : str
        column name containing time period
    strata : str
        column name containing strata information (sic)
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.

    Returns
    -------
    _type_
        pandas dataframe: match counts for each flag column.
    """

    return df.groupby([strata, period])[flag].agg("sum").reset_index()
