import numpy as np
import pandas as pd


def flag_matched_pair_merge(
    df, forward_or_backward, target, period, reference, strata, time_difference=1
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
    df_with_predictive_column = df[[reference, strata, target]]
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
    shift : int
        number of rows to shift up or down
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)

    Returns
    -------
    _type_
        pandas dataframe with column added flagging forward matched pairs and
        predictive target variable data column
    """

    if forward_or_backward == "f":
        shift = shift
    elif forward_or_backward == "b":
        shift = -shift

    df = df.sort_values(by=[reference, period])
    predictive_col_name = forward_or_backward + "_predictive_" + target
    df[[predictive_col_name, "predictive_period"]] = df.groupby(
        [reference, strata]
    ).shift(shift)[[target, period]]

    df["validate_date"] = np.where(
        df[period].dt.month - df["predictive_period"].dt.month == shift, True, False
    )
    matched_col_name = forward_or_backward + "_matched_pair_" + target

    df[matched_col_name] = np.where(
        df[[target, predictive_col_name]].isnull().any(axis=1) | (~df["validate_date"]),
        False,
        True,
    )

    df.drop(["validate_date", "predictive_period"], axis=1, inplace=True)

    return df


def count_matches(df, flag_column_name, period, strata, count_column_name=None):
    """
    Function to count the number of records with matches per period and stratum

    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    flag_column_name : str
        name of column containing flags if a match exists
    period : str
        column name containing time period
    strata : str
        column name containing strata information (sic)
    count_col_name : str, None
        name to give to count column. If `None`, name will be derived based on
        flag column name

    Returns
    -------
    pd.DataFrame
        dataframe with column added for count of records with matches
    """
    if count_column_name is None:
        count_column_name = flag_column_name.split("_")[0] + "_matched_pair_count"
    df[count_column_name] = df.groupby([strata, period])[flag_column_name].transform(
        "sum"
    )
    return df
