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

    fix_default = True
    if fix_default:
        # ToDo: Needs to return the count as this is used to verify
        # What case it is, denom = 0 or undefined?

        number_matches = count_matches(df, match_col, period, strata)
        count_suffix = "_pair_count"
        df = df.merge(
            number_matches, on=[period, strata], suffixes=("", count_suffix), how="left"
        )

        # Handling exception when denominator is 0, sreplaced with nan
        # If denominator is 0, match pairs count should be set to 0

        mask_denominator_zero = (df[link_col].isna()) & (denominator_copy == 0)
        mask_cannot_calculate = (df[link_col].isna()) & (denominator_copy != 0)
        # Cant calculate, set count to None set link to
        # df.loc[mask_cannot_calculate, match_col + count_suffix] = None
        df.loc[mask_cannot_calculate, link_col] = 1

        # Denom is 0, set count to 0, set link to 1
        df.loc[
            mask_denominator_zero,
            match_col + count_suffix,
        ] = 0
        df.loc[mask_denominator_zero, link_col] = 1
        df["default_link_" + match_col] = np.where(
            (mask_cannot_calculate | mask_denominator_zero), True, False
        )

        # df.drop(columns=[match_col + count_suffix], inplace=True)

        ## At the moment, dropping match_count to pass unit tests.
        # Should include match count in wrapper function to then test
        # Expected count outputs

    return df
