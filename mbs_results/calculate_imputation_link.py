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

    denominator.replace(0, np.nan, inplace=True)  # cover division with 0

    df[link_col] = numerator / denominator
    df[link_col] = numerator / denominator

    return df
