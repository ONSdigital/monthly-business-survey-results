import numpy as np
import pandas as pd


def create_turnover_output(
    cp_df: str,
    qv_df: str,
    finalsel_df: str,
    winsorisation_df: str,
    period: str,
    selected_period: int,
) -> pd.DataFrame:
    """
    Returning standardising factor summed by domain for questions 40 and 49.
    Standardising factor estimated using a_weights, o_weights and g_weights.

    Parameters
    ----------
    cp_df : pd.DataFrame
        Reference dataframe with adjusted_value, a_weights, o_weights, and g_weights
    qv_df : pd.DataFrame
        Reference dataframe with adjusted_value, a_weights, o_weights, and g_weights
    finalsel_df : pd.DataFrame
        Reference dataframe with adjusted_value, a_weights, o_weights, and g_weights
    winsorisation_df : pd.DataFrame
        Reference dataframe with adjusted_value, a_weights, o_weights, and g_weights

    Returns
    -------
    pd.DataFrame
        dataframe with estimated_value and without a_weight and g_weight as these are
        not in the turnover analysis output.
    """

    qv_df = qv_df.query("question_no == 40")
    winsorisation_df = winsorisation_df.query(
        "{} == {} and question_no == 40".format(period, selected_period)
    )

    turnover_df = (
        cp_df.merge(qv_df, how="left", on="reference")
        .merge(finalsel_df, how="left", left_on="reference", right_on="ruref")
        .merge(winsorisation_df, how="left", on="reference")
    )

    turnover_df["curr_grossed_value"] = (
        turnover_df["adjusted_value"]
        * turnover_df["design_weight"]
        * turnover_df["outlier_weight"]
        * turnover_df["calibration_factor"]
    )

    # Convert imp_marker to type
    # Type 1: Return, Type 2: Construction, Type 3: Imputation
    type_conditions = [
        turnover_df["imputation_marker"] == "r",
        turnover_df["imputation_marker"].isin(["c", "mc"]),
        turnover_df["imputation_marker"].isin(["fir", "bir", "fic", "fimc"]),
    ]

    type_values = [1, 2, 3]

    turnover_df["type"] = np.select(type_conditions, type_values)

    # The error_res_code column exists in the turnover tool input but is ignored, and
    # its purpose is not known. Adding as a constant zero column to prevent code used
    # for producing tool from erroring.
    turnover_df["error_res_code"] = 0

    turnover_df = turnover_df[
        [
            "sic92",
            "cell_no",
            "reference",
            "entname1",
            "adjusted_value",
            "type",
            "curr_grossed_value",
            "outlier_weight",
            "error_mkr",
            "error_res_code",
            "frotover",
            "froempment",
            "returned_value",
        ]
    ]

    return turnover_df.reset_index(drop=True)
