import pandas as pd

from mbs_results.outlier_detection.calculate_predicted_unit_value import (
    calculate_predicted_unit_value,
)
from mbs_results.outlier_detection.calculate_ratio_estimation import (
    calculate_ratio_estimation,
)
from mbs_results.outlier_detection.calculate_winsorised_weight import (
    calculate_winsorised_weight,
)
from mbs_results.outlier_detection.flag_for_winsorisation import winsorisation_flag


def winsorise(
    df: pd.DataFrame,
    group: str,
    period: str,
    aux: str,
    sampled: str,
    a_weight: str,
    g_weight: str,
    target_variable: str,
    l_values,
) -> pd.DataFrame:
    """
    Applies a technique known as one-sided Winsorisation. The objective of the
    method is to introduce a small bias, while reducing the variance. This is
    intended to reduce the mean squared error of the total, a measure of
    overall accuracy.

    The method uses a pre-calculated parameter, 'L-value' that must be
    supplied to calculate a threshold for each return. The threshold
    calculated depends upon whether expansion or ratio estimation is used in
    this case.

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe
    group : str
        Column name containing group information (sic).
    period : str
        Column name containing time period.
    aux : str
        Column name containing auxiliary variable (x).
    sampled : str
        Column name indicating whether it was sampled or not -boolean.
    a_weight : str
        Column name containing the design weight.
    g_weight:str
        column name containing the g weight.
    target_variable : str
        Column name of the predicted target variable.
    l_values: str
        column name containing the l values as provided by methodology.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the winsorised weights.
    """

    return (
        df.pipe(winsorisation_flag, a_weight, g_weight)
        .pipe(
            calculate_predicted_unit_value,
            group,
            period,
            aux,
            sampled,
            a_weight,
            target_variable,
            "nw_ag_flag",
        )
        .pipe(
            calculate_ratio_estimation,
            aux,
            sampled,
            a_weight,
            g_weight,
            target_variable,
            "predicted_unit_value",
            l_values,
            "nw_ag_flag",
        )
        .pipe(
            calculate_winsorised_weight,
            group,
            period,
            aux,
            sampled,
            a_weight,
            g_weight,
            target_variable,
            "predicted_unit_value",
            l_values,
            "ratio_estimation_treshold",
            "nw_ag_flag",
        )
    )
