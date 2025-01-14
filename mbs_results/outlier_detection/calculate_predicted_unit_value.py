import numpy as np


def calculate_predicted_unit_value(
    df, group, period, aux, is_census, a_weight, target_variable, nw_ag_flag
):
    """
    Calculate predicted unit value

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe.
    group : str
        Column name containing group information (sic).
    period : str
        Column name containing time period.
    aux : str
        Column name containing auxiliary variable (x).
    is_cenus : bool
        Column name indicating whether the reference belongs to a cell that is a census.
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

    winsorised = (~df[is_census]) & (~df[nw_ag_flag])
    filtered_df = df.loc[winsorised]

    filtered_df["weighted_target_values"] = (
        filtered_df[a_weight] * filtered_df[target_variable]
    )
    filtered_df["weighted_auxiliary_values"] = filtered_df[a_weight] * filtered_df[aux]

    sum_weighted_target_values = (
        filtered_df.groupby([group, period])["weighted_target_values"]
        .sum()
        .to_frame(name="sum_weighted_target_values")
        .reset_index()
    )
    sum_weighted_auxiliary_values = (
        filtered_df.groupby([group, period])["weighted_auxiliary_values"]
        .sum()
        .to_frame(name="sum_weighted_auxiliary_values")
        .reset_index()
    )

    total_sum_weighted = sum_weighted_target_values.merge(
        sum_weighted_auxiliary_values, on=[group, period], how="left"
    )

    final_df = df.merge(total_sum_weighted, on=[group, period], how="left")

    final_df["predicted_unit_value"] = (
        final_df[aux]
        * final_df["sum_weighted_target_values"]
        / final_df["sum_weighted_auxiliary_values"]
    )

    final_df = final_df.drop(
        ["sum_weighted_target_values", "sum_weighted_auxiliary_values"], axis=1
    )

    non_winsorised = (final_df[is_census]) | (final_df[nw_ag_flag])
    final_df["predicted_unit_value"] = final_df["predicted_unit_value"].mask(
        non_winsorised, np.nan
    )

    return final_df
