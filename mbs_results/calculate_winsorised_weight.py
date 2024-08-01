import numpy as np
import pandas as pd


def calculate_winsorised_weight(
    df: pd.DataFrame,
    strata: str,
    period: str,
    aux: str,
    sampled: str,
    a_weight: str,
    g_weight: str,
    target_variable: str,
    nw_ag_flag: str,
    predicted_unit_value: str,
    l_values: str,
    ratio_estimation_treshold: str,
) -> pd.DataFrame:

    """
    Calculate winsorised weight

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe.
    period : str
        Column name containing time period.
    strata : str
        Column name containing strata information (sic).
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
    nw_ag_flag: str
        column name indicating whether it can't be winsorised-
        boolean (1 means it can't be winsorised, 0 means it can).
    predicted_unit_value: str
        column name containing the predicted unit value.
    l_values: str
        column name containing the l values as provided by methodology.
    ratio_estimation_treshold: str
        column name containing the previously calculated ratio estimation threshold.


    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the predicted unit value.
    """

    df = df[df["predicted_unit_value"].notna()]
    df = df.reset_index(drop=True)
    # check if reset index creates problems down the line

    df["w"] = df["a_weight"] * df["g_weight"]

    df["new_target"] = df["target_variable"] / df["w"] + (
        df["ratio_estimation_treshold"] - (df["ratio_estimation_treshold"] / df["w"])
    )

    mask = df["target_variable"] <= df["ratio_estimation_treshold"]
    df["new_target_variable"] = np.where(mask, df["target_variable"], df["new_target"])

    df["outlier_weight"] = df["new_target_variable"] / df["target_variable"]

    df = df.drop(["w", "new_target"], axis=1)

    return df
