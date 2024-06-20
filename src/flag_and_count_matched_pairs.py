import numpy as np  # noqa F401
import pandas as pd  # noqa F401


<<<<<<< HEAD

def flag_matched_pair_merge(
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

    if forward_or_backward == "f":
        time_difference = time_difference
    elif forward_or_backward == "b":
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


def flag_matched_pair_shift(
    df, forward_or_backward, target, period, reference, strata, shift=1
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
    time_difference: int
        lookup distance for matched pairs

    Returns
    -------
    _type_
        two pandas dataframes: the main dataframe with column added flagging
        forward matched pairs and
        predictive target variable data column
    """

    df = df.sort_values(by=[reference, period])

    if forward_or_backward == "b":
        time_difference = -time_difference

    df[forward_or_backward + "_match"] = (
        df.groupby([strata, reference])
        .shift(time_difference)[target]
        .notnull()
        .mul(df[target].notnull())
        .mul(
            df[period] - pd.DateOffset(months=time_difference)
            == df.shift(time_difference)[period]
        )
    )

    df.reset_index(drop=True, inplace=True)

    return df


def count_matches(
    df, flag_column_name, period, strata, count_column_name=None, **kwargs
):
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

    Returns
    -------
    _type_
        pandas dataframe: match counts for each flag column.
    """

    return df.groupby([strata, period])[flag].agg("sum").reset_index()
