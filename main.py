import os
print(os.getcwd())
os.chdir("/home/cdsw/monthly-business-survey-results")

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)

# DEBUG - BEGIN


def run_mbs_main():
    config = load_config()
    validate_config(config)

    staged_data = stage_dataframe(config)
    validate_staging(staged_data, config)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    imputation_output = impute(staged_data, config)
    validate_imputation(imputation_output, config)

    # Estimation Wrapper
    estimation_output = estimate(imputation_output, config)
    validate_estimation(estimation_output, config)

    # Outlier Wrapper
    outlier_output = detect_outlier(estimation_output, config)
    validate_outlier_detection(outlier_output, config)

    additional_outputs_df = get_additional_outputs_df(estimation_output, outlier_output)
    produce_additional_outputs(config, additional_outputs_df)


if __name__ == "__main__":
    run_mbs_main()
