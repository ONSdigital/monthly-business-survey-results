import numpy as np
import pandas as pd


def calculate_imputation_link(
    df: pd.DataFrame,
    period: str,
    strata: str,
    match_col: str,
    target_variable: str,
    predictive_variable: str,
) -> pd.DataFrame:
    """
    Calculate link between target_variable and predictive_variable by strata,
    a match_col must be supplied which indicates if target_variable
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
    target_variable : str
        Column name of the targeted variable.
    predictive_variable : str
        Column name of the predicted target variable.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing either f_link or b_link
        based on the input parameters.
    """

    df_intermediate = df.copy()

    if match_col == "f_matched_pair" and predictive_variable == "f_predictive_question":
        link_col_name = "f_link"

    elif (
        match_col == "b_matched_pair" and predictive_variable == "b_predictive_question"
    ):
        link_col_name = "b_link"

    else:
        raise ValueError(
            f"""
        {match_col} and {predictive_variable} do not have same wildcard."""
        )

    df_intermediate[target_variable] = (
        df_intermediate[target_variable] * df_intermediate[match_col]
    )

    df_intermediate[predictive_variable] = (
        df_intermediate[predictive_variable] * df_intermediate[match_col]
    )

    numerator = df_intermediate.groupby([strata, period])[target_variable].transform(
        "sum"
    )

    denominator = df_intermediate.groupby([strata, period])[
        predictive_variable
    ].transform("sum")

    denominator.replace(0, np.nan, inplace=True)  # cover division with 0

    df[link_col_name] = numerator / denominator

    return df
