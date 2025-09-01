import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_selective_editing_outputs,
)
from mbs_results.outputs.selective_editing_validations import (
    qa_selective_editing_outputs,
)
from mbs_results.staging.stage_dataframe import start_of_period_staging
from mbs_results.utilities.utils import get_versioned_filename


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
    imputation_filename = get_versioned_filename("imputation", config)

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
    # copying config to make changes after staging

    config_se = config.copy()
    config_se["selective_editing_period"] = (
        pd.to_datetime(config_se["current_period"], format="%Y%m")
        + pd.DateOffset(months=1)
    ).strftime("%Y%m")
    config_se["revision_window"] = 1

    imputation_output = start_of_period_staging(imputation_output, config_se)

    imputation_output.rename(
        columns={"imputed_and_derived_flag": "imputation_flags_adjustedresponse"},
        inplace=True,
    )

    imputation_output.to_csv(
        config_se["output_path"]
        + f"post_imputation_processing_{config_se['period_selected']}.csv",
        index=False,
    )

    estimation_output = estimate(
        df=imputation_output,
        method="combined",
        convert_NI_GB_cells=True,
        config=config_se,
    )

    estimation_output.to_csv(
        config_se["output_path"]
        + f"se_outputs_estimation_output_{config_se['period_selected']}_testing.csv",
        index=False,
    )

    outlier_output = detect_outlier(estimation_output, config_se)

    outlier_output.to_csv(
        config_se["output_path"]
        + f"se_outputs_outlier_output_{config_se['period_selected']}_testing.csv",
        index=False,
    )

    se_outputs_df = get_additional_outputs_df(estimation_output, outlier_output)

    se_outputs_df.to_csv(
        config_se["output_path"]
        + f"se_outputs_full_df_{config_se['period_selected']}.csv",
        index=False,
    )

    produce_selective_editing_outputs(config_se, se_outputs_df)

    qa_selective_editing_outputs(config_se)
