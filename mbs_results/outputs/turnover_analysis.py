import numpy as np
import pandas as pd


def create_turnover_output(
    additional_outputs_df: pd.DataFrame, sic, **config
) -> pd.DataFrame:
    """
    Creating output for turnover analysis tool.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        estimation input dataframe containing relevant columns for turnover tool
    sic
        Using the SIC value from the main config
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        dataframe in correct format for populating turnover analysis tool.
    """

    aux_info_df = (
        additional_outputs_df[["reference", "period", "runame1", "frotover"]]
        .drop_duplicates()
        .dropna(how="any")
        .merge(
            additional_outputs_df[["reference", "period", "status"]].dropna(how="any"),
            how="left",
            on=["reference", "period"],
        )
        .drop_duplicates()
    )

    turnover_df = (
        additional_outputs_df.copy()
        .query("questioncode == 40")
        .drop(columns=["runame1", "frotover", "status"])
    )

    turnover_df["curr_grossed_value"] = (
        turnover_df["adjustedresponse"]
        * turnover_df["design_weight"]
        * turnover_df["outlier_weight"]
        * turnover_df["calibration_factor"]
        / 1000
    )

    # Also converting adjustedresponse and response to pounds thousands
    turnover_df["adjustedresponse"] = turnover_df["adjustedresponse"] / 1000
    turnover_df["response"] = turnover_df["response"] / 1000

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

    turnover_df = turnover_df.merge(aux_info_df, how="left", on=["reference", "period"])

    turnover_df = turnover_df[
        [
            sic,
            "cell_no",
            "reference",
            "runame1",
            "adjustedresponse",
            "imputed_and_derived_flag",
            "curr_grossed_value",
            "outlier_weight",
            "status",
            "frotover",
            "response",
            "period",
        ]
    ]

    turnover_dict = {}
    for period in turnover_df["period"].unique():
        turnover_dict[str(int(period))] = (
            turnover_df[turnover_df["period"] == period]
            .drop("period", axis=1)
            .reset_index(drop=True)
        )

    return turnover_dict
