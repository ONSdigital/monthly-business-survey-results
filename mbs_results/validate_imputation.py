import pandas as pd


def validate_imputation(df: pd.DataFrame, target: str) -> None:
    """
    Validation for the imputation, including:
    - no missing values in target column

    Parameters
    ----------
    df : pd.DataFrame
        data with imputed values
    target : str
        name of column containing target variable

    Raises
    ------
    """
    if df[target].isna().any():
        raise ValueError(
            f"""
            Target column should have no missing values following imputation:
            missing values found in column {target}
            """
        )
