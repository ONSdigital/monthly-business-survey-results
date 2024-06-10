import numpy as np  # noqa F401
import pandas as pd  # noqa F401


def flag_matched_pair(
    df, forward_or_backward, target, period, reference, strata, time_difference=1
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
    <<<<<<< HEAD
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
    )

    return df


def count_matches(df, flag, period, strata):
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
