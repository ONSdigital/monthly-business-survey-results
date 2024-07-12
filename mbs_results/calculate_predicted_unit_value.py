import pandas as pd


def calculate_predicted_unit_value(
    df: pd.DataFrame,
    period: str,
    strata: str,
    aux: str,
    sampled: str,
    a_weight: str,
    target_variable: str,
    nw_ag_flag: str,
) -> pd.DataFrame:
    """
    Calculate link between target_variable and predictive_variable by strata,
    a match_col must be supplied which indicates if target_variable
    and predictive_variable can be linked.

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
    target_variable : str
        Column name of the predicted target variable.
    nw_ag_flag: str
        column name indicating whether it can't be winsorised-
        boolean (1 means it can't be winsorised, 0 means it can).
    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the predicted unit value.
    """

    df = df.loc[(df["sampled"] == 1) & (df["nw_ag_flag"] == 0)]
    df = df.reset_index(drop=True)
    # check if reset index creates problems down the line

    sum_weighted_target_values = (df["a_weight"] * df["target_variable"]).sum()
    sum_weighted_auxiliary_values = (df["a_weight"] * df["aux"]).sum()

    df["predicted_unit_value"] = df["aux"].apply(
        lambda x: x * (sum_weighted_target_values / sum_weighted_auxiliary_values)
    )

    return df
