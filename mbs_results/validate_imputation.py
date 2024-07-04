import pandas as pd


def validate_imputation(df: pd.DataFrame, target: str) -> None:
    """_summary_

    Parameters
    ----------
    df : pd.DataFrame
        data with imputed values
    target : str
        name of column containing target variable

    Raises
    ------
    """
    if column_missing_values(df[target]):
        raise ValueError(
            f"""
            Target column should have no missing values following imputation:
            missing values found in column {target}
            """
        )


def column_missing_values(target_column: pd.Series) -> bool:
    """_summary_

    Parameters
    ----------
    target_column : pd.Series
        dataframe column to search for missing values

    Returns
    -------
    bool
        True if missing values found, otherwise False
    """
    return target_column.isna().any()
