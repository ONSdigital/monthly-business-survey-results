from mbs_results.outputs.selective_editing_outputs import (
    create_se_outputs,
    load_main_output,
)
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.setup_logger import setup_logger, upload_logger_file_to_s3
from mbs_results.utilities.utils import get_or_create_run_id
from mbs_results.utilities.validation_checks import validate_config


def run_final_outputs(config_user_dict=None):
    config = load_config("config_user.json", config_user_dict)
    config["run_id"] = get_or_create_run_id(config)

    # Initialise the logger at the start of the pipeline
    logger_file_path = f"mbs_final_outputs_{config['run_id']}.log"
    logger = setup_logger(logger_file_path=logger_file_path)
    logger.info(f"MBS Final Outputs started: Log file: {logger_file_path}")

    validate_config(config)

    main_mbs_output = load_main_output(config)
    create_se_outputs(main_mbs_output, config)

    upload_logger_file_to_s3(config, logger_file_path)


if __name__ == "__main__":
    run_final_outputs()
