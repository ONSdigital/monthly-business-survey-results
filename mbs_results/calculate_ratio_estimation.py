import numpy as np


def calculate_ratio_estimation(
    df,
    aux,
    sampled,
    a_weight,
    g_weight,
    target_variable,
    predicted_unit_value,
    l_values,
    nw_ag_flag,
):

    """
    Calculate ratio estimation threshold
    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe.
    aux : str
        Column name containing auxiliary variable (x).
    sampled : str
        Column name indicating whether it was sampled or not -boolean.
    a_weight : str
        Column name containing the design weight.
    g_weight : str
        Column name containing the g weight.
    target_variable : str
        Column name of the predicted target variable.
    predicted_unit_value:
        column name containing the predicted unit value
    l_values:str
        column containing the l values provided by methodology
    nw_ag_flag: str
        column name indicating whether it can't be winsorised-
        boolean (True means it can't be winsorised, False means it can).

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the ratio estimation.
    """

    df["flag_calculation"] = df[a_weight] * df[g_weight]
    df["ratio_estimation_treshold"] = (df[predicted_unit_value]) + (
        df[l_values] / (df["flag_calculation"] - 1)
    )
    df = df.drop("flag_calculation", axis=1)

    non_winsorised = (df[sampled] == 0) | (df[nw_ag_flag] == True)  # noqa: E712
    df["ratio_estimation_treshold"] = df["ratio_estimation_treshold"].mask(
        non_winsorised, np.nan
    )

    return df
