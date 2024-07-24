from typing import Dict

import numpy as np
import pandas as pd

from mbs_results.apply_imputation_link import create_and_merge_imputation_values
from mbs_results.calculate_imputation_link import calculate_imputation_link
from mbs_results.construction_matches import flag_construction_matches
from mbs_results.cumulative_imputation_links import get_cumulative_links
from mbs_results.flag_and_count_matched_pairs import flag_matched_pair
from mbs_results.imputation_flags import generate_imputation_marker
from mbs_results.link_filter import flag_rows_to_ignore
from mbs_results.predictive_variable import shift_by_strata_period


def wrap_flag_matched_pairs(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Wrapper function for flagging forward, backward and construction pair
    matches.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names which were passed to ratio of means function.
    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 4 bool columns, column names are
        forward_or_backward keyword and target column name to distinguish them
    """

    if "ignore_from_link" in df.columns:

        target = default_columns["target"]
        df[f"filtered_{target}"] = df[default_columns["target"]]
        df.loc[df["ignore_from_link"], f"filtered_{target}"] = np.nan
        default_columns = {**default_columns, "target": f"filtered_{target}"}

    flag_arguments = [
        dict(**default_columns, **{"forward_or_backward": "f"}),
        dict(**default_columns, **{"forward_or_backward": "b"}),
    ]

    for args in flag_arguments:

        df = flag_matched_pair(df, **args)

    df = flag_construction_matches(df, **default_columns)

    return df


def wrap_shift_by_strata_period(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Wrapper function for shifting values.

    f_predictive_question: is used from calculate imputation link
    b_predictive_question: is used from calculate imputation link
    f_predictive_auxiliary: is used from create_impute_flags

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names kwargs which were passed to ratio of means function.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 3 new numeric columns which contain the desired
        shifted values.
    """

    target_col = default_columns["target"]

    if "ignore_from_link" in df.columns:

        df["filtered_target"] = df[target_col]

        df.loc[df["ignore_from_link"], "filtered_target"] = np.nan

        default_columns = {**default_columns, "target": "filtered_target"}

    link_arguments = (
        dict(
            **default_columns,
            **{"time_difference": 1, "new_col": "f_predictive_" + target_col},
        ),
        dict(
            **default_columns,
            **{"time_difference": -1, "new_col": "b_predictive_" + target_col},
        ),
        # Needed for create_impute_flags
        dict(
            **{**default_columns, "target": default_columns["auxiliary"]},
            **{"time_difference": 1, "new_col": "f_predictive_auxiliary"},
        ),
    )

    if df.columns.isin(
        ["f_link_question", "b_link_question", "construction_link"]
    ).all():
        link_arguments = link_arguments[2:]

    for args in link_arguments:
        df = shift_by_strata_period(df, **args)

    return df


def wrap_calculate_imputation_link(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """Wrapper for calculate_imputation_link function.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names which were passed to ratio of means function.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 3 new numeric columns which contain the
        imputation links.
    """
    target_col = default_columns["target"]

    if "ignore_from_link" in df.columns:

        df["filtered_target"] = df[target_col]
        df.loc[df["ignore_from_link"], "filtered_target"] = np.nan

        # default_columns = {**default_columns, "target": "filtered_target"}
        # target_col = f"filtered_{target_col}"
    link_arguments = (
        dict(
            **default_columns,
            **{
                "match_col": f"f_match_{target_col}",
                "predictive_variable": "f_predictive_" + target_col,
                "link_col": "f_link_" + target_col,
            },
        ),
        dict(
            **default_columns,
            **{
                "match_col": f"b_match_{target_col}",
                "predictive_variable": "b_predictive_" + target_col,
                "link_col": "b_link_" + target_col,
            },
        ),
        dict(
            **default_columns,
            **{
                "match_col": "flag_construction_matches",
                "predictive_variable": default_columns["auxiliary"],
                "link_col": "construction_link",
            },
        ),
    )

    for args in link_arguments:
        df = calculate_imputation_link(df, **args)

    return df


def wrap_get_cumulative_links(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """Wrapper for calculate_imputation_link function.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names kwargs which were passed to ratio of means function.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 2 new numeric column, with the cumulative
        product of imputation links. These are needed when consecutive periods
        need imputing.
    """
    target_col = default_columns["target"]
    # if f"filtered_{target_col}" in df.columns:
    #     target_col = f"filtered_{target_col}"

    cum_links_arguments = (
        dict(
            **default_columns,
            **{"forward_or_backward": "f", "imputation_link": "f_link_" + target_col},
        ),
        dict(
            **default_columns,
            **{"forward_or_backward": "b", "imputation_link": "b_link_" + target_col},
        ),
    )

    for args in cum_links_arguments:

        df = get_cumulative_links(df, **args)

    return df


def ratio_of_means(
    df: pd.DataFrame,
    target: str,
    period: str,
    reference: str,
    strata: str,
    auxiliary: str,
    filters: pd.DataFrame = None,
    imputation_links: Dict[str, str] = {},
    **kwargs,
) -> pd.DataFrame:
    """
    Imputes for each non-responding contributor a single numeric target
    variable within the dataset for multiple periods simultaneously. It uses
    the relationship between the target variable of interest and a predictive
    value and/or auxiliary variable to inform the imputed value. The method
    can apply forward, backward, construction or forward from construction
    imputation. The type of imputation used will vary for each non-respondent
    in each period depending on whether data is available in the predictive
    period

    Parameters
    ----------
    df : pd.DataFrame
         Original dataframe.
    target : str
        Column name of values to be imputed.
    period : str
        Column name containing datetime information.
    reference : str
        Column name of unique Identifier.
    strata : str
        Column name containing strata information (sic).
    auxiliary : str
        Column name containing auxiliary information (sic).
    filters : pd.DataFrame, optional
        Dataframe with values to exclude from imputation method.
    imputation_links : dict, optional
        Dictionary of column name keys matching to their imputation link value
        ("f_link_question", "b_link_question", "construction_link").
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.

    Returns
    -------
    pd.DataFrame
        Original dataframe with imputed values in the target column, and with
        intermediate columns which were used for the imputation method.
    """

    # Saving args to dict, so we can pass same attributes to multiple functions
    # These arguments are used from the majority of functions

    # TODO: Consider more elegant solution, or define function arguments explicitly

    default_columns = {
        "target": target,
        "period": period,
        "reference": reference,
        "strata": strata,
        "auxiliary": auxiliary,
    }

    if filters is not None:

        df = flag_rows_to_ignore(df, filters)
        # target = f"filtered_{default_columns['target']}"
        # default_columns = {**default_columns, **{"target": f"filtered_{target_col}"}}

    if all(
        links in imputation_links.values()
        for links in ["f_link_question", "b_link_question", "construction_link"]
    ):
        df = df.rename(columns=imputation_links).pipe(
            wrap_shift_by_strata_period, **default_columns
        )

    else:
        df = (
            df.pipe(wrap_flag_matched_pairs, **default_columns)
            .pipe(wrap_shift_by_strata_period, **default_columns)
            .pipe(wrap_calculate_imputation_link, **default_columns)
        )

    if f"{target}_man" in df.columns:
        # Manual Construction
        imputation_types = ("c", "mc", "fir", "bir", "fimc", "fic")
        df["man_link"] = 1

    else:
        imputation_types = ("c", "fir", "bir", "fic")

    df = (
        df  # .pipe(
        #     create_impute_flags,
        #     **default_columns,
        #     predictive_auxiliary="f_predictive_auxiliary"
        # )
        .pipe(generate_imputation_marker, **default_columns)
        .pipe(wrap_get_cumulative_links, **default_columns)
        .pipe(
            create_and_merge_imputation_values,
            **default_columns,
            imputation_class=strata,
            marker=f"imputation_flags_{target}",
            combined_imputation="imputed_value",
            cumulative_forward_link="cumulative_f_link_" + target,
            cumulative_backward_link="cumulative_b_link_" + target,
            construction_link="construction_link",
            imputation_types=imputation_types,
        )
    )

    # TODO: Reset index needed because of sorting, perhaps reset index
    #       when sorting directly in the low level functions or consider
    #       sorting here before chaining
    df.drop(
        columns=["f_match_question", "f_predictive_auxiliary", "b_match_question"],
        inplace=True,
        errors="ignore",
    )
    df = df.reset_index(drop=True)

    # TODO: Relates to ASAP-415, comment from pull request:
    #       You could extact this into its own function which can be called. It might
    #       be nice to switch this on and off incase we need to verify the methods or
    #       methogology needs this. Also potential argument of selecting the needed
    #       columns vs dropping un-needed, guess whichever is a shorter list

    df = df.drop(
        columns=[
            "f_match",
            "b_match",
            "flag_construction_matches",
            "r_flag",
            "fir_flag",
            "bir_flag",
            "c_flag",
            "fic_flag",
            "missing_value",
            "imputation_group",
            "cumulative_f_link_" + target,
            "cumulative_b_link_" + target,
            "f_predictive_" + target,
            "b_predictive_" + target,
            "ignore_from_link",
            "filtered_target",
            "man_link",
        ],
        axis=1,
        errors="ignore",
    )

    # TODO: Missing extra columns, default values and if filter was applied, all bool

    return df
