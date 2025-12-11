import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.new_back_data import export_backdata
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.file_selector import (
    generate_expected_periods,
    validate_files,
)
from mbs_results.utilities.inputs import load_config, read_csv_wrapper
from mbs_results.utilities.outputs import save_df
from mbs_results.utilities.setup_logger import setup_logger, upload_logger_file_to_s3
from mbs_results.utilities.utils import (
    export_run_id,
    generate_schemas,
    get_or_create_run_id,
    get_or_read_run_id,
    get_versioned_filename,
)
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

    # Set up run id
    config["run_id"] = get_or_create_run_id(config)

    # Initialise the logger at the start of the pipeline
    logger_file_path = f"mbs_results_{config['run_id']}.log"
    logger = setup_logger(logger_file_path=logger_file_path)
    logger.info(f"MBS Pipeline Started: Log file: {logger_file_path}")

    df, unprocessed_data, manual_constructions, filter_df = stage_dataframe(config)
    validate_staging(df, config)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    df = impute(df, manual_constructions, config, filter_df)
    validate_imputation(df, config)
    save_df(
        df,
        "imputation",
        config,
        config["debug_mode"],
        config["split_methods_outputs_by_period"],
    )

    # Estimation Wrap
    df = estimate(df=df, method="combined", convert_NI_GB_cells=True, config=config)
    validate_estimation(df, config)
    save_df(
        df,
        "estimation_output",
        config,
        config["debug_mode"],
        config["split_methods_outputs_by_period"],
    )

    # Outlier Wrapper
    df = detect_outlier(df, config)
    validate_outlier_detection(df, config)
    save_df(
        df,
        "outlier_output",
        config,
        config["debug_mode"],
        config["split_methods_outputs_by_period"],
    )

    df = get_additional_outputs_df(df, unprocessed_data, config)
    save_df(
        df,
        "mbs_results",
        config,
        split_by_period=config["split_results_output_by_period"],
    )  # main methods output

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=True, optional_outputs=False, config=config
    )

    generate_schemas(config)

    export_run_id(config["run_id"])

    upload_logger_file_to_s3(config, logger_file_path)


def produce_additional_outputs_wrapper(config_user_dict=None):
    """Produces any additional outputs based on MBS methods output"""

    config = load_config("config_outputs.json", config_user_dict)
    config["run_id"] = get_or_read_run_id(config)

    output_file_name = get_versioned_filename(
        config["mbs_output_prefix"],
        config["run_id"],
    )
    output_path = f"{config['main_mbs_output_folder_path']}{output_file_name}"

    try:
        df = read_csv_wrapper(
            filepath=output_path,
            import_platform=config["platform"],
            bucket_name=config["bucket"],
        )
    except FileNotFoundError:
        expected_dates = generate_expected_periods(
            config["current_period"], config["revision_window"]
        )
        expected_dates_with_run_id = [
            f"{date}_{config['run_id']}" for date in expected_dates
        ]
        valid_files = validate_files(
            file_path=config["main_mbs_output_folder_path"],
            file_prefix=config["mbs_output_prefix"],
            expected_periods=expected_dates_with_run_id,
            config=config,
        )
        if not valid_files:
            raise FileNotFoundError(
                "No mbs output files found to produce additional outputs."
            )
        dataframes = [
            read_csv_wrapper(
                filepath=filepath,
                import_platform=config["platform"],
                bucket_name=config["bucket"],
            )
            for filepath in valid_files
        ]
        df = pd.concat(dataframes, ignore_index=True)

    produce_additional_outputs(
        additional_outputs_df=df, qa_outputs=False, optional_outputs=True, config=config
    )

    export_backdata(df=df, config=config)


if __name__ == "__main__":
    run_mbs_main()
