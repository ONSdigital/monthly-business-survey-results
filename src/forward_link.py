from typing import List

import numpy as np
import pandas as pd


def mask_values(df: pd.DataFrame, target_variable: str, expr: str) -> pd.Series:
    """Convert values in a dataframe column to 0 based on a python expression

    Parameters
    ----------
    df : pd.Dataframe
        Pandas dataframe of original data.
    target_variable : List[str] or str
        Column name(s) containing target variable(s).
    query : str
        The expression to evaluate, see here:
        https://pandas.pydata.org/pandas-docs/version/1.5/reference/api/pandas.eval.html

    Returns
    -------
    df : pd.Series


    """
    masked_column = df[target_variable].copy()

    try:
        masked_column.loc[~(df.eval(expr))] = np.nan

    except ValueError:
        print(
            f"""{expr} is not a valid expression,
        the code uses ~(df.eval({expr}) to mask the dataframe, please see here:
        https://pandas.pydata.org/pandas-docs/version/1.5/reference/api/pandas.eval.html
        """
        )

    return masked_column


def calculate_imputation_link(
    df: pd.DataFrame,
    groups: List[str] or str,
    match_col: str,
    target_variable: str,
    predictive_variable: str,
) -> pd.Series:
    """
    Calculate link between target_variable and predictive_variable by given groups,
    a match_col must be supplied which indicates if target_variable and
    predictive_variable can be linked.

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe.
    groups : List[str] or str
        Column name(s) to calculate the sums.
    match_col : str
        Column of the matched pair links, this column should be bool,
        or 0 and 1.
    target_variable : str
        Column name of the targeted variable.
    predictive_variable : str
        Column name of the predicted target variable.

    Returns
    -------
    link : pd.Series
        A pandas series with the links.
    """

    df_intermediate = df.copy()

    df_intermediate[target_variable] = (
        df_intermediate[target_variable] * df_intermediate[match_col]
    )

    df_intermediate[predictive_variable] = (
        df_intermediate[predictive_variable] * df_intermediate[match_col]
    )

    numerator = df_intermediate.groupby(groups)[target_variable].transform("sum")

    denominator = df_intermediate.groupby(groups)[predictive_variable].transform("sum")

    denominator.replace(0, np.nan, inplace=True)  # cover division with 0

    link = numerator / denominator

    return link
