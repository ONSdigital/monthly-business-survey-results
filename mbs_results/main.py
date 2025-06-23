from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from mbs_results.outputs.qa_output import produce_qa_output
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.utils import get_versioned_filename
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)

# DEBUG - BEGIN


def run_mbs_main(config_user_dict=None):
    config = load_config(config_user_dict)
    validate_config(config)

    staged_data, manual_constructions, filter_df = stage_dataframe(config)
    validate_staging(staged_data, config)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    imputation_output = impute(staged_data, manual_constructions, config, filter_df)
    validate_imputation(imputation_output, config)

    # Estimation Wrapper
    estimation_output = estimate(
        df=imputation_output, method="combined", convert_NI_GB_cells=True, config=config
    )
    validate_estimation(estimation_output, config)

    # Outlier Wrapper
    outlier_output = detect_outlier(estimation_output, config)
    validate_outlier_detection(outlier_output, config)

    # QA output
    qa_output = produce_qa_output(config, outlier_output)
    write_csv_wrapper(
        qa_output,
        config["output_path"] + get_versioned_filename("qa_output", config),
        config["platform"],
        config["bucket"],
        index=False,
    )

    additional_outputs_df = get_additional_outputs_df(estimation_output, outlier_output)
    produce_additional_outputs(config, additional_outputs_df)


if __name__ == "__main__":
    run_mbs_main()
