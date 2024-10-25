from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_outputs import produce_outputs
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)

if __name__ == "__main__":
    config = load_config()
    validate_config(config)

    staged_data = stage_dataframe(config)
    validate_staging(staged_data)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    imputation_output = impute(staged_data, config)
    validate_imputation(imputation_output)

    # Estimation Wrapper
    estimation_output = estimate()
    validate_estimation(estimation_output)

    # Outlier Wrapper
    outlier_output = detect_outlier()
    validate_outlier_detection(outlier_output)

    produce_outputs(outlier_output, "output_path/")
