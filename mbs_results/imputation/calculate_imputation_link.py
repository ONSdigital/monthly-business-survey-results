import numpy as np
import pandas as pd

from mbs_results.imputation.flag_and_count_matched_pairs import count_matches


def calculate_imputation_link(
    df: pd.DataFrame,
    period: str,
    strata: str,
    match_col: str,
    target: str,
    predictive_variable: str,
    link_col: str,
    **kwargs,
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
    if "ignore_from_link" in df.columns:
        # Quick and dirty work around when dealing with filtered cases
        df_intermediate.rename(
            columns={
                f"f_match_filtered_{target}": f"f_match_{target}",
                f"b_match_filtered_{target}": f"b_match_{target}",
            },
            inplace=True,
        )

    df_intermediate[target] = df_intermediate[target] * df_intermediate[match_col]

    df_intermediate[predictive_variable] = (
        df_intermediate[predictive_variable] * df_intermediate[match_col]
    )

    numerator = df_intermediate.groupby([strata, period])[target].transform("sum")

    denominator = df_intermediate.groupby([strata, period])[
        predictive_variable
    ].transform("sum")

    denominator_col = "denominator"

    df[denominator_col] = denominator
    denominator.replace(0, np.nan, inplace=True)  # cover division with 0

    df[link_col] = numerator / denominator

    df = calculate_default_imputation_links(
        df,
        target,
        period,
        strata,
        match_col,
        predictive_variable,
        link_col,
        denominator_col,
    )

    return df


def calculate_default_imputation_links(
    df: pd.DataFrame,
    target: str,
    period: str,
    strata: str,
    match_col: str,
    predictive_variable: str,
    link_col: str,
    denominator: str,
) -> pd.DataFrame:
    """
    Calculates and replaces links to default values when either,
    denominator is zero or the link cannot be calculated.
    Both cases link is replaced with 1
    Matched pair counts is replaced with either 0 (denominator zero)
    or null (link cannot be calculated)


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
    predictive_variable : str
        Column name of the predicted target variable.
    link_col : str
        Name to use for the new column containing imputation link
    denominator : str
        Name used for the column containing the denominators of the link
        calculation

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with default values overwriting values in
        imputation link columns.
    """

    df_intermediate = df.copy()

    # Need this for edge case when denominator is zero
    # But is from a valid zero return. i.e. retuned zero and
    # not a filled zero from empty or Null
    df_intermediate["return_in_predictive_is_zero"] = (
        df_intermediate[predictive_variable] == 0
    )
    grouped_true_zeros = df_intermediate.groupby([strata, period])[
        "return_in_predictive_is_zero"
    ].transform("sum")
    grouped_true_zeros = grouped_true_zeros != 0
    df_intermediate.drop(columns=["return_in_predictive_is_zero"])

    # Re adding count matches column as this is needed for default cases
    # This count is just the filtered target counts if issues come up.
    # (If there is a filter applied to this data)
    if "ignore_from_link" in df.columns and match_col != "flag_construction_matches":
        df.rename(
            columns={
                f"f_match_filtered_{target}": f"f_match_{target}",
                f"b_match_filtered_{target}": f"b_match_{target}",
            },
            inplace=True,
        )
        print(match_col, df.columns)

    number_matches = count_matches(df, match_col, period, strata)
    count_suffix = "_pair_count"
    df = df.merge(
        number_matches, on=[period, strata], suffixes=("", count_suffix), how="left"
    )
    # Creating two logical masks for cases when denominator is zero and
    # link cannot be calculated
    mask_denominator_zero = df[denominator] == 0
    mask_cannot_calculate = ((df[link_col].isna()) | (np.isinf(df[link_col]))) & (
        df[denominator] != 0
    )
    # Default link is always 1:
    df.loc[(mask_cannot_calculate | mask_denominator_zero), link_col] = 1

    df.loc[mask_cannot_calculate, match_col + count_suffix] = None
    df.loc[
        (mask_denominator_zero) & (~grouped_true_zeros), match_col + count_suffix
    ] = 0

    # Creating default link bool column
    df["default_link_" + match_col] = np.where(
        (mask_cannot_calculate | mask_denominator_zero), True, False
    )
    df.drop(columns=[denominator], inplace=True)
    return df
