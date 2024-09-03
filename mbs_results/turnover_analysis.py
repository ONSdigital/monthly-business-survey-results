import numpy as np
import pandas as pd


def create_turnover_output(
    cp_df: str,
    qv_df: str,
    finalsel_df: str,
    winsorisation_df: str,
    winsorisation_period: str,
    selected_period: int,
) -> pd.DataFrame:
    """
    Creating output for turnover analysis tool.

    Parameters
    ----------
    cp_df : pd.DataFrame
        cp input dataframe containing reference, sic and error_mkr
    qv_df : pd.DataFrame
        qv input dataframe containing reference, question_no, adjusted_value and
        returned_value
    finalsel_df : pd.DataFrame
        finalsel input dataframe containing reference, froempment, frotover, cell_no
        and entname1
    winsorisation_df : pd.DataFrame
        winsorisation input dataframe containing question_no, period, reference,
        imputation_marker, design_weight, calibration_factor and outlier_weight
    winsorisation_period : str
        Name of column displaying period in winsorisation
    selected_period : int
        Period to output results for in the format YYYYMM

    Returns
    -------
    pd.DataFrame
        dataframe in correct format for populating turnover analysis tool.
    """

    qv_df = qv_df.query("question_no == 40")
    winsorisation_df = winsorisation_df.query(
        "{} == {} and question_no == 40".format(winsorisation_period, selected_period)
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
