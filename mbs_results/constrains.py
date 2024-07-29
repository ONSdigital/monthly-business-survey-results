import operator
from typing import List

import pandas as pd


def replace_values_index_based(
    df: pd.DataFrame, target: str, a: int, compare: str, b: int
) -> None:
    """
    Perform comparisons between a subset of df with a and subset of df with b and
    replace target from a with target from b when comparison is met. Both a
    and b must exist in the first level index, the comparison is based on the
    remaining indices.

    Note that this function does not return anything, it modifies the input
    dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe to replace the values.
    target : str
        Column name for values to be replced.
    a : int
        Question_no to check.
    compare : str
        Logical operator to compare, accepted '>', '<','>=','<=','=='.
    b : int
        Question_no to check against.
    """
    # For improved perfomance
    df.sort_index(inplace=True)

    ops = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
    }

    series_from_a = df.loc[a][target]

    series_from_b = df.loc[b][target]

    common_index = series_from_a.index.intersection(series_from_b.index)

    # Has format (period,reference)
    index_to_replace = series_from_a[
        ops[compare](series_from_a[common_index], series_from_b[common_index])
    ].index

    if len(index_to_replace) > 0:
        for date_ref_idx in index_to_replace.values:

            # Has format (question,no,period,reference)
            index_to_replace = (a,) + date_ref_idx
            index_to_replace_with = (b,) + date_ref_idx
            # Filter target based on the indices
            df.loc[index_to_replace, target] = df.loc[index_to_replace_with, target]
            df.loc[index_to_replace, "constrain_marker"] = f"{a} {compare} {b}"


def sum_sub_df(df: pd.DataFrame, derive_from: List[int]) -> pd.DataFrame:
    """
    Returns the sums of a dataframe in which the first level index is in
    derive_from. The sum is based on common indices. Columns must contain float
    or int.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to sum, first level index must contain values from derive_from

    derive_from : List[int]
        Values to take a subset of df.

    Returns
    -------
    sums : pd.DataFrame
        A dataframe with sums, constain marker, and columns from index which the
        sum was based on.
    """

    sums = sum(
        [df.loc[question_no] for question_no in derive_from if question_no in df.index]
    )

    return sums.assign(constrain_marker=f"sum{derive_from}").reset_index()
