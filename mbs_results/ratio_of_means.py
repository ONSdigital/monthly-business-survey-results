from typing import Dict

import pandas as pd

from src.apply_imputation_link import create_and_merge_imputation_values
from src.construction_matches import flag_construction_matches
from src.cumulative_imputation_links import get_cumulative_links
from src.flag_and_count_matched_pairs import count_matches, flag_matched_pair_merge
from src.forward_link import calculate_imputation_link
from src.imputation_flags import create_impute_flags, generate_imputation_marker


def flag_all_match_pairs(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Creates bool columns if a predictive values exists for each method (
    forward, backward, construction) also we check if there is predictive
    value for the auxiliary column which is needed for imputation flags.
    This is wrapper for getting matched pairs for each method.
    Note that the created columns are used as inputs in other function(s).

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names kwargs which were passed to ratio of means function.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 4 bool columns, column names are
        forward_or_backward keyword and target column name to distinguish them
    """

    flag_arguments = [
        dict(**default_columns, **{"forward_or_backward": "f"}),
        dict(**default_columns, **{"forward_or_backward": "b"}),
        {
            "forward_or_backward": "f",
            "target": "other",
            "period": "date",
            "reference": "identifier",
            "strata": "group",
        },
    ]

    for args in flag_arguments:

        df = flag_matched_pair_merge(df, **args)

    df = flag_construction_matches(df, **default_columns)

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

    count_arguments = (
        dict(**default_columns, **{"flag_column_name": "f_matched_pair_question"}),
        dict(**default_columns, **{"flag_column_name": "b_matched_pair_question"}),
        dict(**default_columns, **{"flag_column_name": "flag_construction_matches"}),
    )

    for args in count_arguments:
        df = count_matches(df, **args)

    return df


def calculate_all_links(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Creates 3 new numeric columns with the link between target and
    predictive_variable for every imputation method. This is wrapper for
    get_cumulative_links function.

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

    link_arguments = (
        dict(
            **default_columns,
            **{
                "match_col": "f_matched_pair_question",
                "predictive_variable": "f_predictive_question",
            }
        ),
        dict(
            **default_columns,
            **{
                "match_col": "b_matched_pair_question",
                "predictive_variable": "b_predictive_question",
            }
        ),
        {
            "period": "date",
            "strata": "group",
            "match_col": "flag_construction_matches",
            "target": "question",
            "predictive_variable": "other",
        },
    )

    for args in link_arguments:
        df = calculate_imputation_link(df, **args)

    return df


def calculate_all_cum_links(
    df: pd.DataFrame, **default_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Creates 2 new numeric columns with the cumulative product link, forward
    uses ffill and backward bffill.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    **default_columns : Dict[str, str]
        The column names kwargs which were passed to ratio of means function.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with 2 new numeric columns.
    """

    cum_links_arguments = (
        dict(
            **default_columns,
            **{"forward_or_backward": "f", "imputation_link": "f_link_question"}
        ),
        dict(
            **default_columns,
            **{"forward_or_backward": "b", "imputation_link": "b_link_question"}
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

    Returns
    -------
    pd.DataFrame
        Original dataframe with imputed values in the target column, and with
        intermediate columns which were used for the imputation method.
    """

    # Saving args to dict, so we can pass same attributes to multiple functions

    default_columns = locals()

    del default_columns["df"]

    # TODO: Is datetime needed? If so here is a good place to convert to datetime

    df = (
        df.pipe(flag_all_match_pairs, **default_columns)
        .pipe(count_all_matches, **default_columns)
        .pipe(calculate_all_links, **default_columns)
        .pipe(
            create_impute_flags,
            **default_columns,
            predictive_auxiliary="f_predictive_other"
        )
        .pipe(generate_imputation_marker)
        .pipe(calculate_all_cum_links, **default_columns)
        .pipe(
            create_and_merge_imputation_values,
            **default_columns,
            imputation_class="group",
            marker="imputation_marker",
            combined_imputation="imputed_value",
            cumulative_forward_link="cumulative_f_link_question",
            cumulative_backward_link="cumulative_b_link_question",
            construction_link="construction_link",
            imputation_types=("c", "fir", "bir", "fic")
        )
    )

    df = df.drop(
        columns=[
            "f_matched_pair_question",
            "b_matched_pair_question",
            "f_matched_pair_other",
            "flag_construction_matches",
            "r_flag",
            "fir_flag",
            "bir_flag",
            "c_flag",
            "fic_flag",
            "missing_value",
            "imputation_group",
            "cumulative_f_link_question",
            "cumulative_b_link_question",
        ]
    )

    # TODO: Missing extra columns, default values and if filter was applied, all bool

    return df
