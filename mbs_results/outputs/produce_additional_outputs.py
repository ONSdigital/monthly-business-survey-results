from importlib import metadata

import pandas as pd

from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.outputs.ocea_srs_outputs import produce_ocea_srs_outputs
from mbs_results.outputs.pivot_imputation_value import create_imputation_link_output
from mbs_results.outputs.selective_editing_contributer_output import (
    get_selective_editing_contributor_output,
)
from mbs_results.outputs.selective_editing_question_output import (
    create_selective_editing_question_output,
)
from mbs_results.outputs.turnover_analysis import create_turnover_output
from mbs_results.outputs.weighted_adj_val_time_series import (
    get_weighted_adj_val_time_series,
)
from mbs_results.utilities.utils import get_versioned_filename


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
            "converted_frotover",
            "calibration_factor",
            "adjustedresponse",
            "status",
            "response",
            "froempment",
            "cell_no",
            "imputation_class",
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
            "turnover_output": create_turnover_output,
            "weighted_adj_val_time_series": get_weighted_adj_val_time_series,
            "produce_ocea_srs_outputs": produce_ocea_srs_outputs,
            "create_imputation_link_output": create_imputation_link_output,
        },
        additional_outputs_df,
    )

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    for output in additional_outputs:
        filename = get_versioned_filename(output, config)
        additional_outputs[output].to_csv(config["output_path"] + filename, index=False)
        print(config["output_path"] + filename + " saved")


def produce_selective_editing_outputs(
    config: dict, additional_outputs_df: pd.DataFrame
):
    """
    Function to write selective editing outputs

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
            "selective_editing_contributors": get_selective_editing_contributor_output,
            "selective_editing_questions": create_selective_editing_question_output,
        },
        additional_outputs_df,
    )

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]

    for output in additional_outputs:
        file = output.split("_")[-1]
        period = additional_outputs[output]["period"].unique()[0]
        filename = f"se{file}009_{period}_v{file_version_mbs}.csv"
        additional_outputs[output].to_csv(config["output_path"] + filename, index=False)
        print(config["output_path"] + filename + " saved")
