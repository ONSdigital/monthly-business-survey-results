import numpy as np
import pandas as pd

from mbs_results.flag_and_count_matched_pairs import count_matches

# from flag_and_count_matched_pairs import count_matches


def calculate_imputation_link(
    df: pd.DataFrame,
    period: str,
    strata: str,
    match_col: str,
    target: str,
    predictive_variable: str,
    link_col: str,
    **kwargs
) -> pd.DataFrame:
    """
    Calculate link between target and predictive_variable by strata,
    a match_col must be supplied which indicates if target
    and predictive_variable can be linked.

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe.
    period : str
        Column name containing time period.
    strata : str
        Column name containing strata information (sic).
    match_col : str
        Column name of the matched pair links, this column should be bool.
    target : str
        Column name of the targeted variable.
    predictive_variable : str
        Column name of the predicted target variable.
    link_col : str
        Name to use for the new column containing imputation link
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing imputation link.
    """

    df_intermediate = df.copy()

    df_intermediate[target] = df_intermediate[target] * df_intermediate[match_col]

    df_intermediate[predictive_variable] = (
        df_intermediate[predictive_variable] * df_intermediate[match_col]
    )

    numerator = df_intermediate.groupby([strata, period])[target].transform("sum")

    denominator = df_intermediate.groupby([strata, period])[
        predictive_variable
    ].transform("sum")

    denominator_copy = denominator.copy()
    denominator.replace(0, np.nan, inplace=True)  # cover division with 0

    df[link_col] = numerator / denominator

    # Re adding count matches column as this is needed for default cases
    number_matches = count_matches(df, match_col, period, strata)
    count_suffix = "_pair_count"
    df = df.merge(
        number_matches, on=[period, strata], suffixes=("", count_suffix), how="left"
    )
    # Creating two logical masks for cases when denominator is zero and
    # link cannot be calculated
    mask_denominator_zero = ((df[link_col].isna()) | (np.isinf(df[link_col]))) & (
        denominator_copy == 0
    )
    mask_cannot_calculate = ((df[link_col].isna()) | (np.isinf(df[link_col]))) & (
        denominator_copy != 0
    )
    # Default link is always 1:
    df.loc[(mask_cannot_calculate | mask_denominator_zero), link_col] = 1
    # Cant calculate, set count to None set link to
    # setting to Null fails unit tests

    df.loc[
        mask_cannot_calculate, match_col + count_suffix
    ] = 0  # Setting to None fails tests
    df.loc[mask_denominator_zero, match_col + count_suffix] = 0

    # Creating default link bool column
    df["default_link_" + match_col] = np.where(
        (mask_cannot_calculate | mask_denominator_zero), True, False
    )

    return df
