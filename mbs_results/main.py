from mbs_results import configure_logger_with_run_id, logger
from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.inputs import load_config, read_csv_wrapper
from mbs_results.utilities.outputs import save_df
from mbs_results.utilities.utils import export_run_id, generate_schemas, read_run_id
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)


def run_mbs_main(config_user_dict=None):
    """Main function to run MBS methods pipeline"""

    # Early logging uses MemoryHandler (buffers until config ready)
    logger.info("Starting MBS pipeline...")

    config = load_config("config_user.json", config_user_dict)
    # Now configure full logging with run_id and file handler
    configure_logger_with_run_id(config)
    logger.info("Config loaded, logger configured with run_id")

    validate_config(config)

    df, unprocessed_data, manual_constructions, filter_df = stage_dataframe(config)
    validate_staging(df, config)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    df = impute(df, manual_constructions, config, filter_df)
    validate_imputation(df, config)
    save_df(df, "imputation", config, config["debug_mode"])
    # Estimation Wrap
    df = estimate(df=df, method="combined", convert_NI_GB_cells=True, config=config)
    validate_estimation(df, config)
    save_df(df, "estimation_output", config, config["debug_mode"])

    # Outlier Wrapper
    df = detect_outlier(df, config)
    validate_outlier_detection(df, config)
    save_df(df, "outlier_output", config, config["debug_mode"])

    df = get_additional_outputs_df(df, unprocessed_data, config)
    save_df(df, "mbs_results", config)  # main methods output

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=True, optional_outputs=False, config=config
    )

    generate_schemas(config)

    export_run_id(config["run_id"])


def produce_additional_outputs_wrapper(config_user_dict=None):
    """Produces any additional outputs based on MBS methods output"""

    config = load_config("config_outputs.json", config_user_dict)
    config["run_id"] = read_run_id()

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
