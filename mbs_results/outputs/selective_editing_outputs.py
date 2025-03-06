from importlib import metadata

import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import get_additional_outputs_df
from mbs_results.outputs.selective_editing_contributer_output import (
    get_selective_editing_contributer_output,
)
from mbs_results.outputs.selective_editing_question_output import (
    create_selective_editing_question_output,
)
from mbs_results.staging.stage_dataframe import start_of_period_staging
from mbs_results.utilities.validation_checks import qa_selective_editing_outputs


def load_imputation_output(config: dict) -> pd.DataFrame:
    """
    Loads the imputation output from a CSV file.

    Parameters
    ----------
    config : dict
        A dictionary containing configuration parameters.
        - "output_path" (str): The path to the directory containing the output files.
        - "mbs_file_name" (str): The name of the MBS file used to generate the
        imputation output.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the imputation output data.
    """
    output_path = config["output_path"]
    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    imputation_filename = f"imputation_output_v{file_version_mbs}_{snapshot_name}.csv"

    imputation_output = pd.read_csv(output_path + imputation_filename)

    return imputation_output


def create_se_outputs(imputation_output: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Creates and saves selective editing outputs from the imputation output DataFrame.

    Parameters
    ----------
    imputation_output : pd.DataFrame
        The DataFrame containing the imputation output data.
    config : dict
        A dictionary containing configuration parameters.
        - "output_path" (str): The path to the directory where the output files will
        be saved.
        - "mbs_file_name" (str): The name of the MBS file used to generate the
        imputation output.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the selective editing outputs.
    """
    imputation_output = start_of_period_staging(imputation_output, config)

    # Create missing questions

    estimation_output = estimate(imputation_output, config)

    outlier_output = detect_outlier(estimation_output, config)

    se_outputs_df = get_additional_outputs_df(estimation_output, outlier_output)

    contributer = get_selective_editing_contributer_output(se_outputs_df, **config)
    question = create_selective_editing_question_output(se_outputs_df, **config)

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]

    contributer_filename = (
        f"se_contributer_output_v{file_version_mbs}_{snapshot_name}.csv"
    )
    contributer.to_csv(config["output_path"] + contributer_filename, index=False)
    print(config["output_path"] + contributer_filename + " saved")

    question_filename = f"se_question_output_v{file_version_mbs}_{snapshot_name}.csv"
    question.to_csv(config["output_path"] + question_filename, index=False)
    print(config["output_path"] + question_filename + " saved")

    qa_selective_editing_outputs(config)
