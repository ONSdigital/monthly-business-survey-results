import numpy as np
import pandas as pd


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

    df[link_col] = numerator / denominator

    df = calculate_default_imputation_links(df, denominator, match_col, link_col)

    return df


def calculate_default_imputation_links(
    df: pd.DataFrame,
    denominator: pd.Series,
    match_col: str,
    link_col: str,
    default_value: int = 1,
) -> pd.DataFrame:
    """
    Replaces links to default values when either,
    denominator is zero or the link cannot be calculated.
    Both cases link is replaced with 1 and a new column is created to
    indicate which rows were affected.

    Note that it the link cannot be calculated because of lack of matched
    pairs them the sum will be 0. So essentially defaults are used when
    demonator is 0.

    From the link is not possible to distinguish the above 2 cases, since both
    have link of 1 and denominator is 0, users should refer to counts for that.

    If counts are 0 then default link was used because of no matched pairs,
    if counts > 0 then default link was used because of sum of predictive
    variable was 0.

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe.
    denominator : Series
        A series with the demonator of the link calculation.
    match_col : str
        Column name of the matched pair links, this column should be bool.
    link_col : str
        Name to use for the new column containing imputation link
    default_link : int, optional
        Value to use for default links. The default is 1.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with default values overwriting values in
        imputation link columns.
    """
    default_indices = denominator.index[denominator == 0].tolist()

    df.loc[default_indices, link_col] = default_value

    df["default_link_" + match_col] = np.where(
        df.index.isin(default_indices), True, False
    )

    return df
