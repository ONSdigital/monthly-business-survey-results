from typing import Dict

import numpy as np
import pandas as pd

from mbs_results.imputation.apply_imputation_link import (
    create_and_merge_imputation_values,
)
from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.imputation.cumulative_imputation_links import get_cumulative_links
from mbs_results.imputation.flag_and_count_matched_pairs import (
    count_matches,
    flag_matched_pair,
)
from mbs_results.imputation.imputation_flags import generate_imputation_marker
from mbs_results.imputation.link_filter import flag_rows_to_ignore
from mbs_results.imputation.predictive_variable import shift_by_strata_period
from mbs_results.staging.data_cleaning import join_manual_constructions


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

    target = default_columns["target"]

    if "ignore_from_link" in df.columns:

        df[f"filtered_{target}"] = df[default_columns["target"]]
        df.loc[df["ignore_from_link"], f"filtered_{target}"] = np.nan
        default_columns = {**default_columns, "target": f"filtered_{target}"}

    #   if "imputation_flag" in df.columns:
    default_columns = {
        **default_columns,
        "imputation_flag": f"imputation_flags_{target}",
    }

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


def process_backdata(
    df: pd.DataFrame, target: str, period: str, back_data_period: str
) -> pd.DataFrame:
    """
    function to process the back data. Removes some values from target column so
    correct imputation links are calculated

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe
    target : str
        tartget column name
    period : str
        period column name
    back_data_period : str
        back data period value

    Returns
    -------
    pd.DataFrame
        dataframe with backdata processed and backdata flags copied to seperate columns
    """
    # Bool for if period is back data
    df["is_backdata"] = df[period] == pd.to_datetime(back_data_period, format="%Y%m")
    # Copying backdata to seperate column
    df.loc[df["is_backdata"], f"backdata_{target}"] = df.loc[df["is_backdata"], target]
    # Copying flags to sep column
    df[f"backdata_flags_{target}"] = df[f"imputation_flags_{target}"].str.lower()

    # moving mc data into manual construction column for MC imputation
    df.loc[df[f"backdata_flags_{target}"] == "mc", f"{target}_man"] = df.loc[
        df[f"backdata_flags_{target}"] == "mc", target
    ]
    df.loc[df[f"backdata_flags_{target}"] == "fimc", f"{target}_man"] = df.loc[
        df[f"backdata_flags_{target}"] == "fimc", target
    ]

    # removing mc data from target column
    df.loc[
        (~df[f"backdata_flags_{target}"].isin(["r"]))
        & (df[f"backdata_flags_{target}"].notna()),
        target,
    ] = None

    return df


def reapply_backdata(
    df: pd.DataFrame, target: str, dropping: bool = False
) -> pd.DataFrame:
    """
    reapply backdata flags and values to ensure no changes are made to back data.
    will not do anything if backdata is not present in dataframe
    dropping is optional argument which will drop the copied backdata column

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe
    target : str
        target column name
    dropping : bool, optional
        if true the temp column to store back data will be removed , by default False

    Returns
    -------
    pd.DataFrame
        original dataframe with back data re-applied.
    """
    if f"backdata_flags_{target}" in df.columns:

        is_backdata_not_return = (df[f"backdata_flags_{target}"] != "r") & (
            df["is_backdata"]
        )
        df.loc[is_backdata_not_return, target] = df.loc[
            is_backdata_not_return, f"backdata_{target}"
        ]
        df.loc[is_backdata_not_return, f"imputation_flags_{target}"] = df.loc[
            is_backdata_not_return, f"backdata_flags_{target}"
        ]

    if dropping:
        df.drop(columns=["is_backdata"], inplace=True)

    return df


def replace_fir_backdata(df: pd.DataFrame, target: str) -> pd.DataFrame:
    """
    replaced the target column with back data.
    this is removed before calculating forwards and backwards links to
    ensure the correct values are used.

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe
    target : str
        target column name

    Returns
    -------
    pd.DataFrame
        original dataframe with imputed data copied over into the target column.

    """
    if f"backdata_flags_{target}" in df.columns:
        df.loc[(df[f"backdata_flags_{target}"].isin(["fir"])), target] = df.loc[
            (df[f"backdata_flags_{target}"].isin(["fir"])), f"backdata_{target}"
        ]

    return df


def count_all_matches(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Creates 3 new numeric columns with the sum of matches between targeted and
    predicted values for each imputation method. This is wrapper for
    count_matches function.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names kwargs which were passed to ratio of means function.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 3 new numeric columns.
    """

    target_col_name = default_columns["target"]

    if "ignore_from_link" in df.columns:

        target_col_name = "filtered_" + target_col_name

    count_arguments = (
        dict(**default_columns, **{"flag": "f_match_" + target_col_name}),
        dict(**default_columns, **{"flag": "b_match_" + target_col_name}),
        dict(**default_columns, **{"flag": "flag_construction_matches"}),
    )

    for args in count_arguments:
        df = count_matches(df, **args)

    return df


def ratio_of_means(
    df: pd.DataFrame,
    target: str,
    period: str,
    reference: str,
    strata: str,
    auxiliary: str,
    current_period: int,
    revision_window: int,
    question_no: str,
    filters: pd.DataFrame = None,
    manual_constructions: pd.DataFrame = None,
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
    current_period: int
        Value with current period to be imputed as int.
    revision_window: int
        Value containing the amount of periods for imputation.
    question_no: str
        Column name containing question_no
    filters : pd.DataFrame, optional
        Dataframe with values to exclude from imputation method.
    manual_constructions : pd.DataFrame, optional
        Dataframe with values which are used for manual construction
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
    back_data_period = calculate_back_data_period(current_period, revision_window)
    if f"imputation_flags_{target}" in df.columns:
        df = process_backdata(df, target, period, back_data_period)

    default_columns = {
        "target": target,
        "period": period,
        "reference": reference,
        "strata": strata,
        "auxiliary": auxiliary,
        "back_data_period": back_data_period,
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
            .pipe(count_all_matches, **default_columns)
        )

    if manual_constructions is not None:
        # Need to join mc dataframe to original df
        df = join_manual_constructions(
            df, manual_constructions, reference, period, question_no, target
        )

    if f"{target}_man" in df.columns:
        # Manual Construction
        imputation_types = ("c", "mc", "fir", "bir", "fimc", "fic")
        df["man_link"] = 1

    else:
        imputation_types = ("c", "fir", "bir", "fic")

    df = (
        df
        # Pass backdata period to calculate imputation link
        .pipe(replace_fir_backdata, target=target)
        .pipe(generate_imputation_marker, **default_columns)
        .pipe(wrap_get_cumulative_links, **default_columns)
        .pipe(reapply_backdata, target=target)
        .pipe(
            create_and_merge_imputation_values,
            **default_columns,
            imputation_class=strata,
            marker=f"imputation_flags_{target}",
            cumulative_forward_link="cumulative_f_link_" + target,
            cumulative_backward_link="cumulative_b_link_" + target,
            construction_link="construction_link",
            imputation_types=imputation_types,
        )
        .pipe(reapply_backdata, target=target)
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


def calculate_back_data_period(current_period, revision_window) -> str:
    current_period = pd.to_datetime(current_period, format="%Y%m")
    back_data_period = (
        (current_period - pd.DateOffset(months=revision_window)).date().strftime("%Y%m")
    )
    return back_data_period
