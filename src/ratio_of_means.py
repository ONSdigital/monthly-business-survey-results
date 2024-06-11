from typing import Dict

import pandas as pd

from construction_matches import flag_construction_matches
from flag_and_count_matched_pairs import flag_matched_pair_merge


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
