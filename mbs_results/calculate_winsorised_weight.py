import numpy as np
import pandas as pd

from mbs_results.data_cleaning import (
    convert_column_to_datetime,
    validate_manual_constructions,
)


def calculate_winsorised_weight(
    df,
    group,
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


def load_l_values(df: pd.DataFrame, reference, period, l_values_path, **config):
    """
    function to load the l values data and merge onto main df

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with combined responses and contributors columns
        index is expected to be 'period' and 'reference'
    reference: str
        the name of the reference column
    period: str
        the name of the period column
    l_values_path: str
        the location of the l values data
    **config: Dict
        main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        dataframe with correctly formatted column datatypes.
    """
    l_values = pd.read_csv(l_values_path)
    l_values[period] = convert_column_to_datetime(l_values[period])
    l_values[reference] = l_values[reference].astype("str")
    l_values.set_index([reference, period], inplace=True)

    validate_manual_constructions(df, l_values)

    return df.merge(
        l_values,
        on=[reference, period],
        how="outer",
    )


def validate_l_values(df, l_values):
    """
    Checks that manual construction identifiers match those in the main dataset

    Parameters
    ----------
    df: pd.DataFrame
        the main dataframe after preprocessing
    manual_constructions: pd.DataFrame
        the manual constructions input read in as a dataframe

    Raises
    ------
    ValueError
        ValueError if any combinations of period and reference appear in the manual
        constructions input but not in the main dataframe
    """

    incorrect_ids = set(l_values.index) - set(df.index)

    if len(incorrect_ids) > 1:
        string_ids = " ".join(
            [f"\nreference: {str(i[0])}, period: {str(i[1])}" for i in incorrect_ids]
        )

        raise ValueError(
            f"""There are reference and period combinations in the manual constructions
      with no match: {string_ids}"""
        )
