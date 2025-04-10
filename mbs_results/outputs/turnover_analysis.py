import numpy as np
import pandas as pd


def create_turnover_output(
    additional_outputs_df: pd.DataFrame, current_period: int,sic,  **config
) -> pd.DataFrame:
    """
    Creating output for turnover analysis tool.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        estimation input dataframe containing relevant columns for turnover tool
    current_period : int
        Period of output results in the format YYYYMM
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary
    sic
        Using the SIC value from the main config

    Returns
    -------
    pd.DataFrame
        dataframe in correct format for populating turnover analysis tool.
    """

    turnover_df = additional_outputs_df.query(
        "period == {} and questioncode == 40".format(current_period)
    )

    turnover_df["curr_grossed_value"] = (
        turnover_df["adjustedresponse"]
        * turnover_df["design_weight"]
        * turnover_df["outlier_weight"]
        * turnover_df["calibration_factor"]
    )

    # Convert imp_marker to type
    # Type 1: Return, Type 2: Construction, Type 3: Imputation
    type_conditions = [
        turnover_df["imputation_flags_adjustedresponse"] == "r",
        turnover_df["imputation_flags_adjustedresponse"].isin(["c", "mc"]),
        turnover_df["imputation_flags_adjustedresponse"].isin(
            ["fir", "bir", "fic", "fimc"]
        ),
    ]

    type_values = [1, 2, 3]

    turnover_df["type"] = np.select(type_conditions, type_values)

    # Check if referencename in data
    if "referencename" in turnover_df.columns:

        turnover_df = turnover_df[
            [
                sic,
                "cell_no",
                "reference",
                "referencename",
                "adjustedresponse",
                "type",
                "curr_grossed_value",
                "outlier_weight",
                "status",
                "frotover",
                "response",
            ]
        ]
    elif "referencename" not in turnover_df.columns:

        turnover_df["referencename"] = ""

        turnover_df = turnover_df[
            [
                sic,
                "cell_no",
                "reference",
                "referencename",
                "adjustedresponse",
                "type",
                "curr_grossed_value",
                "outlier_weight",
                "status",
                "frotover",
                "response",
            ]
        ]

    # output turnover analysis dataframe
    return turnover_df.reset_index(drop=True)
