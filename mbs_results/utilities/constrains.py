import logging
import operator
import warnings
from typing import List

import pandas as pd

from mbs_results.utilities.inputs import read_csv_wrapper
from mbs_results.utilities.validation_checks import validate_manual_outlier_df

logger = logging.getLogger(__name__)


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
    index_to_replace = series_from_a[common_index][
        ops[compare](series_from_a[common_index], series_from_b[common_index])
    ].index

    if len(index_to_replace) > 0:
        for date_ref_idx in index_to_replace.values:

            # Has format (question_no,period,reference)
            index_to_replace = (a,) + date_ref_idx
            index_to_replace_with = (b,) + date_ref_idx

            # Convert to series to ensure consistent dtype
            # df.loc[index_to_replace_with, target] could be either series or float

            replace_with_value = pd.Series(df.loc[index_to_replace_with, target]).iloc[
                0
            ]
            df.loc[index_to_replace, target] = replace_with_value
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
    # difference between using sum or agg RE NaNs
    # Temp fix, fillna with 0.
    # Add info the backlog ticket to replace this temp fix after combining columns
    df_temp = df.fillna(0)

    sums = sum(
        [
            df_temp.loc[question_no]
            for question_no in derive_from
            if question_no in df_temp.index
        ]
    )

    return sums.assign(constrain_marker=f"sum{derive_from}").reset_index()


def constrain(
    df: pd.DataFrame,
    period: str,
    reference: str,
    target: str,
    question_no: str,
    spp_form_id: str,
    sic: str,
) -> pd.DataFrame:
    """
    Creates new rows with derived values based on form id and adds a relevant
    marker to constain_marker column (is created if not existing).

        For form id 13, question number 40 is created by summing 46,47.
        For form id 14, question number 40 is created by summing 42,43.
        For form id 15, question number 46 is created from 40.
            Question number 47 with derived value of 0 is also created from 40.
        For form id 16, question number 42 is created from 40.
            Question number 43 with derived value of 0 also created

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
        Column name containing target value and imputed values.
    question_no : str
        Column name containing question number.
    spp_form_id : str
        Column name containing form id.
    sic: str
        Calls in the SIC value from the Main config

    Returns
    -------
    final_constrained : pd.DataFrame
        Original dataframe with constrains.
    """
    derive_map, derive_map_null = create_derive_map(df, spp_form_id)

    df[f"pre_derived_{target}"] = df[target]

    # pre_derive_df has dimensions as index, columns the values to be used when derived
    # Hard coded columns are from finalsel files,
    pre_derive_df = df.set_index(
        [
            spp_form_id,
            question_no,
            period,
            reference,
            "cell_no",
            "converted_frotover",
            "froempment",
            sic,
            "formtype",
        ],
        verify_integrity=False,
    )

    pre_derive_df = pre_derive_df[[target]]

    derived_values_list = [
        sum_sub_df(pre_derive_df.loc[form_type], derives["from"])
        .assign(**{question_no: derives["derive"]})
        .assign(**{spp_form_id: form_type})
        for form_type, derives in derive_map.items()
    ]

    derived_null_values_list = [
        sum_sub_df(pre_derive_df.loc[form_type], derives["from"])
        .assign(**{question_no: derives["derive"]})
        .assign(**{spp_form_id: form_type})
        .assign(**{target: 0})
        .assign(**{"constrain_marker": "Zero for winsorisation"})
        for form_type, derives in derive_map_null.items()
    ]

    if derived_values_list:
        derived_values = pd.concat(derived_values_list)
    else:
        warnings.warn("No derived questions created")
        derived_values = pd.DataFrame(columns=["constrain_marker"])

    if derived_null_values_list:
        derived_null_values = pd.concat(derived_null_values_list)
    else:
        warnings.warn("No derived questions with zero value created")
        derived_null_values = pd.DataFrame(columns=["constrain_marker"])

    pre_constrained = pd.concat([df, derived_values, derived_null_values])
    pre_constrained[f"pre_constrained_{target}"] = pre_constrained[target]

    unique_q_numbers = pre_constrained[question_no].unique()
    pre_constrained.set_index([question_no, period, reference], inplace=True)

    if 49 in unique_q_numbers:
        replace_values_index_based(pre_constrained, target, 49, ">", 40)

    if 90 in unique_q_numbers:
        replace_values_index_based(pre_constrained, target, 90, ">=", 40)

    post_constrained = pre_constrained.copy().reset_index()

    return post_constrained


def derive_questions(
    df: pd.DataFrame,
    period: str,
    reference: str,
    target: str,
    question_no: str,
    spp_form_id: str,
    config: dict,
) -> pd.DataFrame:
    """
    Function to calculate new o-weights post winsorisation
    This has same functionality has constraints, but does not use
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
    derive_map, _ = create_derive_map(df, spp_form_id)

    pre_derive_df = df.set_index(
        [
            spp_form_id,
            question_no,
            period,
            reference,
            "cell_no",
            "converted_frotover",
            "froempment",
            config["sic"],
            "formtype",
        ],
        verify_integrity=False,
    )

    # Assuming default value of o-weight is 1
    pre_derive_df = pre_derive_df[[target]].fillna(value=0)

    derived_values_list = [
        sum_sub_df(pre_derive_df.loc[form_type], derives["from"])
        .assign(**{question_no: derives["derive"]})
        .assign(**{spp_form_id: form_type})
        # Create a task on Backlog to fix this.
        for form_type, derives in derive_map.items()
    ]

    if derived_values_list:
        derived_values = pd.concat(derived_values_list)

    else:
        warnings.warn("No derived questions created")
        derived_values = pd.DataFrame(columns=["constrain_marker"])

    unique_q_numbers = df[question_no].unique()

    df.set_index([question_no, period, reference], inplace=True)

    # This would replace 49 with 40, but might have been winsorised independently
    if all(num in unique_q_numbers for num in [40, 49]):
        replace_values_index_based(df, target, 49, ">", 40)
    elif all(num in unique_q_numbers for num in [40, 90]):
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
    (dict, dict)
        Derived question mapping in tuple of dict.
        First dict in the tuple contains derived question mappings for real values.
        Second dict in the tuple contains derived question mappings for null values.
        Removes form IDs which are not present in dataframe
    """

    derive_map = {
        13: {"derive": 40, "from": [46, 47]},
        14: {"derive": 40, "from": [42, 43]},
        15: {"derive": 46, "from": [40]},  # Needs to derive 46 and 47
        16: {"derive": 42, "from": [40]},  # Needs to derive 42 and 43
    }

    derive_map_null = {
        15: {"derive": 47, "from": [40]},
        16: {"derive": 43, "from": [40]},
    }

    form_ids_present = df[spp_form_id].dropna().unique()

    ids_not_present = [x for x in derive_map.keys() if x not in form_ids_present]
    for key in ids_not_present:
        derive_map.pop(key)

    ids_not_present = [x for x in derive_map_null.keys() if x not in form_ids_present]
    for key in ids_not_present:
        derive_map_null.pop(key)

    return derive_map, derive_map_null


def calculate_derived_outlier_weights(
    df: pd.DataFrame,
    period: str,
    reference: str,
    target: str,
    question_no: str,
    spp_form_id: str,
    outlier_weight: str,
    winsorised_target: str,
    config: dict,
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
    df.loc[default_o_weight_bool, winsorised_target] = df.loc[
        default_o_weight_bool, target
    ]

    if "constrain_marker" not in df.columns:
        # Handling case where derived Q's not been
        # calculated pre-winsorisation
        df_pre_winsorised = derive_questions(
            df,
            period,
            reference,
            target,
            question_no,
            spp_form_id,
            config,
        )
    else:
        # Skipping calculating derived Q's
        df_pre_winsorised = df.copy()

    post_win_derived = derive_questions(
        df,
        period,
        reference,
        winsorised_target,
        question_no,
        spp_form_id,
        config,
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

    updated_o_weight_bool = df_pre_winsorised[winsorised_target].isna()

    df_pre_winsorised.loc[updated_o_weight_bool, winsorised_target] = (
        post_win_derived.loc[updated_o_weight_bool, winsorised_target]
    )
    df_pre_winsorised["post_wins_marker"] = updated_o_weight_bool

    df_pre_winsorised.reset_index(inplace=True)
    df_pre_winsorised.loc[
        df_pre_winsorised["constrain_marker"].notna(), outlier_weight
    ] = (df_pre_winsorised[winsorised_target] / df_pre_winsorised[target])

    df_pre_winsorised.loc[
        (df_pre_winsorised["constrain_marker"].notna())
        & (df_pre_winsorised[target] == 0),
        outlier_weight,
    ] = 1

    df_pre_winsorised.sort_values(
        by=[reference, period, question_no, spp_form_id], inplace=True
    )

    return df_pre_winsorised


def update_derived_weight_and_winsorised_value(
    df: pd.DataFrame,
    reference: str,
    period: str,
    question_code: str,
    form_type_spp: str,
    outlier_weight: str,
    target: str,
    tolerance=5,
) -> pd.DataFrame:
    """Updates outlier weights and winsorised values to match  the components

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    reference : str
        Column name containing reference.
    period : str
        Column name containing period.
    question_code : str
        Column name containing question code.
    form_type_spp : str
        Column name containing form type spp.
    outlier_weight : str
        Column name containing outlier weight (refered also as o-weight).
    target : str
        Column name containing target value.
    tolerance: int
        Tolerance to check if update should take place, if the absolute
        difference of winsorised value and sum of components is less than
        10**(-tolerance) post_winsorised will be set to False.
    Returns
    -------
    df : pd.Dataframe
        Original dataframe with weights and winsorised values updated to match
        components.
    """
    derive_map, _ = create_derive_map(df, form_type_spp)

    derived_all = []

    df["winsorised_value"] = df[outlier_weight] * df[target]

    for spp_id in derive_map:

        df_spp_id = df[df[form_type_spp] == spp_id].copy()

        df_spp_id = df_spp_id.pivot(
            index=[reference, period],
            columns=question_code,
            values=["winsorised_value", target],
        )

        df_spp_id["post_winsorised"] = df_spp_id["winsorised_value"][
            derive_map[spp_id]["derive"]
        ] != df_spp_id["winsorised_value"][derive_map[spp_id]["from"]].sum(axis=1)

        df_spp_id["post_win_o_weight"] = (
            df_spp_id["winsorised_value"][derive_map[spp_id]["from"]].sum(axis=1)
            / df_spp_id[target][derive_map[spp_id]["derive"]]
        )

        df_spp_id["post_winsorised_value"] = df_spp_id["winsorised_value"][
            derive_map[spp_id]["from"]
        ].sum(axis=1)

        df_spp_id = df_spp_id[
            ["post_winsorised", "post_win_o_weight", "post_winsorised_value"]
        ]

        df_spp_id[question_code] = derive_map[spp_id]["derive"]

        df_spp_id.columns = df_spp_id.columns.droplevel(1)

        df_spp_id.reset_index(inplace=True)

        derived_all.append(df_spp_id)

    # case when nothing to update
    if not derived_all:
        df["post_winsorised"] = False
        return df

    # post_win_derives has all references period question codes which need updating
    # unique values are identified by reference period questioncode

    post_win_derives = pd.concat(derived_all)

    df = pd.merge(
        left=df,
        right=post_win_derives,
        how="left",
        on=[reference, period, question_code],
    )

    df.loc[
        abs(df["post_winsorised_value"] - df["winsorised_value"])
        <= pow(10, -tolerance),
        "post_winsorised",
    ] = False

    # fill na with false
    df["post_winsorised"] = df["post_winsorised"].fillna(0).astype("bool")
    if f"imputation_flags_{target}" in df.columns:
        se_exceptions = (
            df[f"imputation_flags_{target}"] != "manual copy previous period value"
        )
    else:
        se_exceptions = True

    df.loc[(df["post_winsorised"]) & se_exceptions, outlier_weight] = df[
        "post_win_o_weight"
    ]

    df.loc[(df["post_winsorised"]) & se_exceptions, "winsorised_value"] = df[
        "post_winsorised_value"
    ]

    df.drop(columns=["post_win_o_weight", "post_winsorised_value"], inplace=True)

    return df


def replace_with_manual_outlier_weights(
    df: pd.DataFrame,
    reference: str,
    period: str,
    question_code: str,
    outlier_weight: str,
    config: dict,
) -> pd.DataFrame:
    """
    Overwrite calculated outlier weights with manual outlier weights

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    reference : str
        Column name containing reference.
    period : str
        Column name containing period.
    question_code : str
        Column name containing question code.
    outlier_weight : str
        Column name containing outlier weight (refered also as o-weight).
    manual_outlier_weight : str
        Column name containing manual outlier weight, ingested from the
        manual outliers file
    config : dict
        Dictionary containing the following keys of interest:
        platform - either "s3" or "network"
        manual_outlier_path: String containing file path to manual outliers
                             file.
        bucket_name - S3 bucket name for file storage. (optional)

    Returns
    -------
    df : pd.Dataframe
        Original dataframe with weights updated to equal those supplied
        in the manual outliers file, if it exists.
    """
    if not config["manual_outlier_path"]:
        warnings.warn(
            "No manual outlier file has been specified in the configuration,"
            " skipping stage"
        )
        manual_outlier_df = None

        return df

    else:
        manual_outlier_df = read_csv_wrapper(
            config["manual_outlier_path"], config["platform"], config["bucket"]
        )
        validate_manual_outlier_df(manual_outlier_df, reference, period, question_code)

        # Use an outer join to log unmatched manual outlier weights
        unmatched_df = df.merge(
            manual_outlier_df,
            how="outer",
            on=[reference, period, question_code],
            indicator=True,
        )

        unmatched_df = unmatched_df[unmatched_df["_merge"] == "right_only"]
        unmatched_df = unmatched_df[
            [reference, period, question_code, "manual_outlier_weight"]
        ]

        if len(unmatched_df) > 0:
            logger.warning(
                f"\nThere are {len(unmatched_df)} unmatched references in the"
                " ingested manual outlier data"
                "\nUnmatched references:\n"
                f"{unmatched_df}"
            )

        # Use left join to combine manual outlier weights with derived weights
        df = df.merge(
            manual_outlier_df,
            how="left",
            on=[reference, period, question_code],
        )

        # Create pre_manual_outlier column that is a copy of outlier_weight
        df["pre_manual_outlier"] = df[outlier_weight]

        # Overwrite outlier_weight with manual_outlier, if it exists for that record
        df.loc[~df["manual_outlier_weight"].isna(), outlier_weight] = df[
            "manual_outlier_weight"
        ]

        df = df.drop(columns=["manual_outlier_weight"])

        return df
