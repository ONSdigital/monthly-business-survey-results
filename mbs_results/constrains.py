import operator
from typing import List

import pandas as pd


def replace_values_index_based(
    df: pd.DataFrame, target: str, a: int, compare: str, b: int
) -> None:
    """
    Perform comparisons between the dataframe which has `a` in first level index and
    the dataframe which has `b`  in the first level index and replace target when
    condition  is met. Both `a` and `b` must exist in the first level index, 
    the comparison is based on the remaining indices.

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

            # Has format (question_no,period,reference)
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


def constrain(
    df: pd.DataFrame,
    period: str,
    reference: str,
    target: str,
    target_imputed: str,
    question_no: str,
    spp_form_id: str,
) -> pd.DataFrame:
    """
    Creates new rows with derived values based on form id and adds a relevant
    marker to constain_marker column (is created if not existing).

        For form id 13, question number 40 is created by summing 46,47.
        For form id 14, question number 40 is created by summing 42,43.
        For form id 15, question number 46 is created from 40.
        For form id 16, question number 42 is created from 40.

    In addition for all form types (when question number is available):

        Replaces 49 with 40 when 49 > 40.
        Replaces 90 with 40 when 90 >= 40.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe to apply the constrains.
    period : str
        Column name containing date information.
    reference : str
        Column name containing reference.
    target : str
        Column name containing target value.
    target_imputed : str
        Column name containing imputed target value.
    question_no : str
        Column name containing question number.
    spp_form_id : str
        Column name containing form id.

    Returns
    -------
    final_constrained : pd.DataFrame
        Original dataframe with constrains.
    """

    derive_map = {
        13: {"derive": 40, "from": [46, 47]},
        14: {"derive": 40, "from": [42, 43]},
        15: {"derive": 46, "from": [40]},
        16: {"derive": 42, "from": [40]},
    }

    # pre_derive_df has dimenesions as index, columns the values to be used when derived
    pre_derive_df = df.set_index(
        [spp_form_id, question_no, period, reference], verify_integrity=False
    )
    pre_derive_df = pre_derive_df[[target, target_imputed]]

    derived_values = pd.concat(
        [
            sum_sub_df(pre_derive_df.loc[form_type], derives["from"]).assign(
                question_no=derives["derive"]
            )
            for form_type, derives in derive_map.items()
        ]
    )

    df.set_index([question_no, period, reference], inplace=True)

    replace_values_index_based(df, target_imputed, 49, ">", 40)
    replace_values_index_based(df, target_imputed, 90, ">=", 40)

    df.reset_index(inplace=True)

    final_constrained = pd.concat([df, derived_values])

    return final_constrained
