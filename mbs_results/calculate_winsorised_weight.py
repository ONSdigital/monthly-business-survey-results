import numpy as np


def calculate_winsorised_weight(
    df,
    strata,
    period,
    aux,
    sampled,
    a_weight,
    g_weight,
    target_variable,
    predicted_unit_value,
    l_values,
    ratio_estimation_treshold,
    nw_ag_flag,
):

    """
    Calculate winsorised weight

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe
    strata : str
        Column name containing strata information (sic).
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
    predicted_unit_value: str
        column name containing the predicted unit value.
    l_values: str
        column name containing the l values as provided by methodology.
    ratio_estimation_treshold: str
        column name containing the previously calculated ratio estimation threshold.
    nw_ag_flag: str
        column name indicating whether it can't be winsorised-
        boolean (True means it can't be winsorised, False means it can).


    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the winsorised weights.
    """

    df["w"] = df[a_weight] * df[g_weight]

    df["new_target"] = (df[target_variable] / df["w"]) + (
        df[ratio_estimation_treshold] - (df[ratio_estimation_treshold] / df["w"])
    )

    mask = df[target_variable] <= df[ratio_estimation_treshold]
    df["new_target_variable"] = np.where(mask, df[target_variable], df["new_target"])

    df["outlier_weight"] = df["new_target_variable"] / df[target_variable]

    df = df.drop(["w", "new_target"], axis=1)

    non_winsorised = (df[sampled] == 0) | (df[nw_ag_flag] == True)  # noqa: E712
    df["outlier_weight"] = df["outlier_weight"].mask(non_winsorised, np.nan)
    df["new_target_variable"] = df["new_target_variable"].mask(non_winsorised, np.nan)

    return df
