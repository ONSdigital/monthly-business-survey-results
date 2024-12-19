import numpy as np


def calculate_winsorised_weight(
    df,
    group,
    period,
    aux,
    is_census,
    a_weight,
    g_weight,
    target_variable,
    predicted_unit_value,
    ratio_estimation_treshold,
    nw_ag_flag,
):
    """
    Calculate winsorised weight

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
        Column name indicating whether a reference belongs to a cell that is a census.
    a_weight : str
        Column name containing the design weight.
    g_weight:str
        column name containing the g weight.
    target_variable : str
        Column name of the predicted target variable.
    predicted_unit_value: str
        column name containing the predicted unit value.
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

    non_winsorised = (df[is_census]) | (df[nw_ag_flag])

    division_with_0 = ~non_winsorised & (df[target_variable] == 0)

    df["outlier_weight"] = df["outlier_weight"].mask(
        non_winsorised | division_with_0, 1
    )

    df["new_target_variable"] = df["new_target_variable"].mask(
        non_winsorised | division_with_0, np.nan
    )

    return df
