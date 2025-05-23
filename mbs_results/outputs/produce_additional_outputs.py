from importlib import metadata

import pandas as pd

from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.outputs.selective_editing_contributer_output import (
    get_selective_editing_contributer_output,
)
from mbs_results.outputs.selective_editing_question_output import (
    create_selective_editing_question_output,
)
from mbs_results.outputs.turnover_analysis import create_turnover_output
from mbs_results.outputs.weighted_adj_val_time_series import (
    get_weighted_adj_val_time_series,
)


def get_additional_outputs_df(
    estimation_output: pd.DataFrame, outlier_output: pd.DataFrame
):
    """
    Creating dataframe that contains all variables needed for producing additional
    outputs.

    Parameters
    ----------
    estimation_output : pd.DataFrame
        Dataframe output from the estimation stage of the pipeline
    outlier_output : pd.DataFrame
        Dataframe output from the outliering stage of the pipeline

    Returns
    -------
    pd.DataFrame

    """

    additional_outputs_df = estimation_output[
        [
            "reference",
            "period",
            "design_weight",
            "frosic2007",
            "formtype",
            "questioncode",
            "frotover",
            "calibration_factor",
            "adjustedresponse",
            "status",
            "response",
            "froempment",
            "cell_no",
            "referencename",
            "imputation_flags_adjustedresponse",
            "f_link_adjustedresponse",
            "b_link_adjustedresponse",
            "construction_link",
        ]
    ]

    additional_outputs_df = additional_outputs_df.merge(
        outlier_output[["reference", "period", "questioncode", "outlier_weight"]],
        how="left",
        on=["reference", "period", "questioncode"],
    )

    return additional_outputs_df


def produce_additional_outputs(config: dict, additional_outputs_df: pd.DataFrame):
    """
    Function to write additional outputs

    Parameters
    ----------
    config : Dict
        main pipeline configuration
    additional_outputs_df : pd.DataFrame
        Dataframe to feed in as arguments for additional outputs

    Returns
    -------
    None.
        Outputs are written to output path defined in config

    """

    additional_outputs = get_additional_outputs(
        config,
        {
            "selective_editing_contributor": get_selective_editing_contributer_output,
            "selective_editing_question": create_selective_editing_question_output,
            "turnover_output": create_turnover_output,
            "weighted_adj_val_time_series": get_weighted_adj_val_time_series,
        },
        additional_outputs_df,
    )

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    for output in additional_outputs:
        filename = f"{output}_v{file_version_mbs}_{snapshot_name}.csv"
        additional_outputs[output].to_csv(config["output_path"] + filename)
