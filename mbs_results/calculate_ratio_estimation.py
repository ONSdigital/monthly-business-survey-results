import pandas as pd


def calculate_ratio_estimation(
    df: pd.DataFrame,
    strata: str,
    period: str,
    aux: str,
    sampled: str,
    a_weight: str,
    g_weight: str,
    target_variable: str,
    predicted_unit_value: str,
    l_values: str,
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
    g_weight : str
        Column name containing the g weight.
    target_variable : str
        Column name of the predicted target variable.
    predicted_unit_value:
        column name containing the predicted unit value
    l_values:str
        column containing the l values provided by methodology
    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the predicted unit value.
    """

    df = df[df["predicted_unit_value"].notna()]
    df = df.reset_index(drop=True)
    # check if reset index creates problems down the line

    df["flag_calculation"] = df["a_weight"] * df["g_weight"]
    df["ratio_estimation_treshold"] = df["predicted_unit_value"] + (
        df["l_values"] / (df["flag_calculation"] - 1)
    )
    df = df.drop("flag_calculation", axis=1)

    return df
