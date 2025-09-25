from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.inputs import load_config, read_csv_wrapper
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.utils import get_versioned_filename
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)


def run_mbs_main(config_user_dict=None):
    """Main function to run MBS methods pipeline"""

    config = load_config("config_user.json", config_user_dict)
    validate_config(config)

    df, unprocessed_data, manual_constructions, filter_df = stage_dataframe(config)
    validate_staging(df, config)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    df = impute(df, manual_constructions, config, filter_df)
    validate_imputation(df, config)

    # Estimation Wrapper
    df = estimate(df=df, method="combined", convert_NI_GB_cells=True, config=config)
    validate_estimation(df, config)

    # Outlier Wrapper
    df = detect_outlier(df, config)
    validate_outlier_detection(df, config)

    df = get_additional_outputs_df(df, unprocessed_data, config)

    mbs_filename = get_versioned_filename("mbs_results", config)

    write_csv_wrapper(
        df,
        config["output_path"] + mbs_filename,
        config["platform"],
        config["bucket"],
        index=False,
    )

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=True, optional_outputs=False, config=config
    )


def produce_additional_outputs_wrapper(config_user_dict=None):
    """Produces any additional outputs based on MBS methods output"""

    config = load_config("config_outputs.json", config_user_dict)

    df = read_csv_wrapper(
        filepath=config["mbs_output_path"],
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=False, optional_outputs=True, config=config
    )


if __name__ == "__main__":
    run_mbs_main()
