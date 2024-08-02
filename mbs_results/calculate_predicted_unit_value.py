import numpy as np


def calculate_predicted_unit_value(
    df, aux, sampled, a_weight, target_variable, nw_ag_flag
):
    """
    Calculate predicted unit value

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
    target_variable : str
        Column name of the predicted target variable.
    nw_ag_flag: str
        column name indicating whether it can't be winsorised-
        boolean (True means it can't be winsorised, False means it can).
    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the predicted unit value.
    """

    winsorised = (df[sampled] == 1) & (df[nw_ag_flag] == False)  # noqa: E712
    filtered_df = df.loc[winsorised]

    sum_weighted_target_values = (
        filtered_df[a_weight] * filtered_df[target_variable]
    ).sum()
    sum_weighted_auxiliary_values = (filtered_df[a_weight] * filtered_df[aux]).sum()

    df["predicted_unit_value"] = df[aux].apply(
        lambda x: x * (sum_weighted_target_values / sum_weighted_auxiliary_values)
    )

    non_winsorised = (df[sampled] == 0) | (df[nw_ag_flag] == True)  # noqa: E712
    df["predicted_unit_value"] = df["predicted_unit_value"].mask(non_winsorised, np.nan)

    return df
