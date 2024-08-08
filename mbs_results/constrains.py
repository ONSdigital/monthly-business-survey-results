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
    # print([df.loc[question_no] for question_no in derive_from if question_no in df.index])
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

    derive_map = create_derive_map(df, spp_form_id)

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

    if 49 in df[question_no].unique():
        replace_values_index_based(df, target_imputed, 49, ">", 40)
    elif 90 in df[question_no].unique():
        replace_values_index_based(df, target_imputed, 90, ">=", 40)

    df.reset_index(inplace=True)

    final_constrained = pd.concat([df, derived_values])

    return final_constrained


def derive_questions(
    df: pd.DataFrame,
    period: str,
    reference: str,
    target: str,
    question_no: str,
    spp_form_id: str,
) -> pd.DataFrame:
    """
    Function to calculate new o-weights post winsorisation
    This has same functionality has constraints, but doesn't not use
    two target variable columns, refactoring could be done to combine
    further down the line
    ASSUMES DEFAULT O-WEIGHT IS 1

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe, can be with or without constrain_marker column
        Function will create this if not previously existing
    period : str
        Column name containing date information.
    reference : str
        Column name containing reference.
    target : str
        Column name containing target value.
    question_no : str
        Column name containing question number.
    spp_form_id : str
        Column name containing form id.

    Returns
    -------
    pd.DataFrame
        Original dataframe with constrains.
    """
    derive_map = create_derive_map(df, spp_form_id)
    # print(spp_form_id)
    # df.rename(columns = {spp_form_id: "spp_form_id"},inplace=True)
    # print(df.columns)
    pre_derive_df = df.set_index(
        [spp_form_id, question_no, period, reference], verify_integrity=False
    )
    # Assuming default value of o-weight is 1
    pre_derive_df = pre_derive_df[[target]].fillna(value=0)

    # print(pre_derive_df.dtypes)
    derived_values = pd.concat(
        [
            sum_sub_df(pre_derive_df.loc[form_type], derives["from"])
            .assign(question_no=derives["derive"])
            .assign(spp_form_id=form_type)
            # Create a task on Backlog to fix this.
            for form_type, derives in derive_map.items()
        ]
    )
    unique_q_numbers = df.question_no.unique()

    df.set_index([question_no, period, reference], inplace=True)

    # This would replace 49 with 40, but might have been winsorised independently
    if [40, 49] in unique_q_numbers:
        replace_values_index_based(df, target, 49, ">", 40)
    elif [90, 40] in unique_q_numbers:
        replace_values_index_based(df, target, 90, ">=", 40)
    df.reset_index(inplace=True)
    

    final_constrained = pd.concat([df, derived_values]).reset_index(drop=True)
    # final_constrained.rename(columns ={"spp_form_id": spp_form_id},inplace=True)
    return final_constrained


def create_derive_map(df: pd.DataFrame, spp_form_id: str):
    """
    Function to create derive mapping dictionary
    Will check the unique values for form types and remove this
    from the dictionary if not present. handles error

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe
    spp_form_id : str
        Column name containing form id.

    Returns
    -------
    dict
        Derived question mapping in a dictionary.
        Removes form IDs which are not present in dataframe
    """

    derive_map = {
        13: {"derive": 40, "from": [46, 47]},
        14: {"derive": 40, "from": [42, 43]},
        15: {"derive": 46, "from": [40]},
        16: {"derive": 42, "from": [40]},
    }
    form_ids_present = df[spp_form_id].dropna().unique()
    ids_not_present = [x for x in derive_map.keys() if x not in form_ids_present]
    for key in ids_not_present:
        derive_map.pop(key)

    return derive_map


def calculate_derived_outlier_weights(
    df: pd.DataFrame,
    period: str,
    reference: str,
    target: str,
    question_no: str,
    spp_form_id: str,
    outlier_weight: str,
    winsorised_target: str,
) -> pd.DataFrame:
    """
    Function to calculate new outlier weights for derived questions
    post winsorisation. This function can work if constrain_marker is not
    already present and will derive this from `target_variable`
    This will be skipped if column already exists.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe, can be with or without constrain_marker column
        Function will create this if not previously existing
    period : str
        Column name containing date information.
    reference : str
        Column name containing reference.
    target : str
        Column name containing target value.
    question_no : str
        Column name containing question number.
    spp_form_id : str
        Column name containing form id.
    outlier_weight : str
        column name containing outlier weights from winsorisation
    winsorised_target : str
        column name for winsorised target variable

    -------
    pd.DataFrame
        Original dataframe with updated outlier weights calculated for
        derived questions. Bool column is also added to identify which
        rows have been modified during this function
    """
    default_o_weight_bool = df[winsorised_target].isna()
    df["default_o_weight"] = default_o_weight_bool

    # Assuming default value of o-weight is 1
    df.loc[
        default_o_weight_bool, winsorised_target
    ] = df.loc[default_o_weight_bool, target]

    if "constrain_marker" not in df.columns:
        # Handling case where derived Q's not been
        # calculated pre-winsorisation
        print("here")
        df_pre_winsorised = derive_questions(
            df,
            period,
            reference,
            target,
            question_no,
            spp_form_id,
        )
    else:
        print("else here")
        # Skipping calculating derived Q's
        df_pre_winsorised = df.copy()
    # print(df_pre_winsorised.columns)

    post_win_derived = derive_questions(
        df,
        period,
        reference,
        winsorised_target,
        question_no,
        spp_form_id,
    )

    post_win_derived = post_win_derived.loc[
        post_win_derived["constrain_marker"].notna()
    ]
    post_win_derived = post_win_derived[
        [period, reference, question_no, spp_form_id, winsorised_target]
    ]

    df_pre_winsorised.set_index(
        [spp_form_id, question_no, period, reference], inplace=True
    )
    post_win_derived.set_index(
        [spp_form_id, question_no, period, reference], inplace=True
    )
    # print(df_pre_winsorised.head())
    # print(post_win_derived.head())

    updated_o_weight_bool = df_pre_winsorised[winsorised_target].isna()
    df_pre_winsorised.loc[
        updated_o_weight_bool, winsorised_target
    ] = post_win_derived.loc[updated_o_weight_bool, winsorised_target]
    df_pre_winsorised["post_wins_marker"] = updated_o_weight_bool

    df_pre_winsorised.reset_index(inplace=True)
    # print()
    df_pre_winsorised.loc[
        df_pre_winsorised["constrain_marker"].notna(), outlier_weight
    ] = (df_pre_winsorised[winsorised_target] / df_pre_winsorised[target])
    df_pre_winsorised.sort_values(
        by=[reference, period, question_no, spp_form_id], inplace=True
    )

    return df_pre_winsorised


if __name__ == "__main__":
    df = pd.read_csv(
        "tests/data/winsorisation/derived-questions-winsor.csv", index_col=False
    )

    df_input = df.drop(df[df["question_no"] == 40].index)
    target_variable_col = "target_variable"
    df_input[target_variable_col] = df_input[target_variable_col].astype(float)
    df_input["outlier_weight"] = df_input["outlier_weight"].astype(float)
    print("input", df_input)
    df_output = calculate_derived_outlier_weights(
        df_input,
        "period",
        "reference",
        target_variable_col,
        "question_no",
        "spp_form_id",
        "outlier_weight",
        "new_target_variable",
    )

    print("output", df_output)

    df = pd.read_csv(
        "tests/data/winsorisation/derived-questions-winsor-missing.csv", index_col=False
    )

    df_input = df.drop(df[df["question_no"] == 40].index)
    target_variable_col = "target_variable"
    df_input[target_variable_col] = df_input[target_variable_col].astype(float)
    df_input["outlier_weight"] = df_input["outlier_weight"].astype(float)
    print("input Missing values \n", df_input)
    df_output = calculate_derived_outlier_weights(
        df_input,
        "period",
        "reference",
        target_variable_col,
        "question_no",
        "spp_form_id",
        "outlier_weight",
        "new_target_variable",
    )

    print("output_missing \n", df_output)

    large_df = pd.read_csv("C:/Users/dayj1/Downloads/post_win.csv", index_col=False)
    large_df.drop(columns=["constrain_marker"],inplace=True)
    large_df.rename(columns = {"form_type_spp":"spp_form_id"},inplace=True)

    target_variable_col = "adjusted_value"

    df_output = calculate_derived_outlier_weights(
        large_df,
        "period_x",
        "reference",
        target_variable_col,
        "question_no",
        "spp_form_id",
        "outlier_weight",
        "new_target_variable"
    )
    print(df_output)

#  TO do, find reference where q40 is summed and both / either are winsorised 
# Maybe set o-weight to 1 where default_o_weight true?
