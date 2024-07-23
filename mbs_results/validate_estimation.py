import pandas as pd


def validate_estimation(
    df: pd.DataFrame, design_weight: str, calibration_factor: str, auxiliary: str
) -> None:
    """
    Validation for the estimation, including:
    - no missing values in weight columns
    - weighted sum equal to unweighted sum

    Parameters
    ----------
    df : pd.DataFrame
        data with estimation weights
    design_weight : str
        name of column in df containing design weight
    calibration_weight : str
        name of column in df containing calibration factor
    auxiliary : str
        name of column in df containing auxiliary variable

    Raises
    ------
    `ValueError`
    """
    for column in [design_weight, calibration_factor]:
        if df[column].isna().any():
            raise ValueError(
                f"""
Weight column should have no missing values following estimation:
missing values found in column {column}
                """
            )

    weighted_sum = (df[auxiliary] * df[design_weight] * df[calibration_factor]).sum()
    unweighted_sum = df[auxiliary].sum()

    if weighted_sum != unweighted_sum:
        raise ValueError(
            f"""
Sum of auxiliary variable multiplied by design weight and calibration factor
should be equal to sum of auxiliary variable.
Auxiliary: {auxiliary}
Unweighted sum: {unweighted_sum}
Weighted sum: {weighted_sum}
            """
        )
