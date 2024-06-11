from typing import Dict

import pandas as pd

from construction_matches import flag_construction_matches
from flag_and_count_matched_pairs import count_matches, flag_matched_pair_merge


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
