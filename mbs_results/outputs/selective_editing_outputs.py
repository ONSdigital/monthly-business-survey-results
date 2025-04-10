import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_selective_editing_outputs,
)
from mbs_results.staging.stage_dataframe import start_of_period_staging
from mbs_results.utilities.utils import get_versioned_filename
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
    imputation_filename = get_versioned_filename("imputation_output", config)

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

    imputation_output.rename(
        columns={"imputed_and_derived_flag": "imputation_flags_adjustedresponse"},
        inplace=True,
    )

    imputation_output.to_csv(
        config["output_path"]
        + f"post_imputation_processing_{config['period_selected']}.csv",
        index=False,
    )

    estimation_output = estimate(imputation_output, config)

    outlier_output = detect_outlier(estimation_output, config)

    se_outputs_df = get_additional_outputs_df(estimation_output, outlier_output)

    se_outputs_df.to_csv(
        config["output_path"] + f"se_outputs_full_df_{config['period_selected']}.csv",
        index=False,
    )

    produce_selective_editing_outputs(config, se_outputs_df)

    qa_selective_editing_outputs(config)
