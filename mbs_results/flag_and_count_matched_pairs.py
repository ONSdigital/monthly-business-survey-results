import numpy as np  # noqa F401
import pandas as pd  # noqa F401


def flag_matched_pair(
    df,
    forward_or_backward,
    target,
    period,
    reference,
    strata,
    time_difference=1,
    **kwargs
):
    """
    function to add flag to df if data forms a matched pair
    i.e. data is given for both period and predictive period
    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    forward_or_backward: str
        either f or b for forward or backward method
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)
    time_difference : int
        time difference between predictive and target period in months


    Returns
    -------
    pd.DataFrame
        dataframe with column added flagging forward matched paris and
        predictive target variable data column
    """

    df = df.sort_values(by=[strata, reference, period])

    if forward_or_backward == "b":
        time_difference = -time_difference

    # Creating new DF, shifting period for forward or backward
    df_with_predictive_column = df.copy()[[reference, strata, target]]
    df_with_predictive_column["predictive_period"] = df[period] + pd.DateOffset(
        months=time_difference
    )
    predictive_col_name = forward_or_backward + "_predictive_" + target
    df_with_predictive_column.rename(
        columns={target: predictive_col_name}, inplace=True
    )

    df = df.merge(
        df_with_predictive_column,
        left_on=[reference, period, strata],
        right_on=[reference, "predictive_period", strata],
        how="left",
    )

    matched_col_name = forward_or_backward + "_matched_pair_" + target

    df[matched_col_name] = np.where(
        df[[target, predictive_col_name]].isnull().any(axis=1), False, True
    )

    df.drop(["predictive_period"], axis=1, inplace=True)
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
