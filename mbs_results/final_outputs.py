from mbs_results.outputs.selective_editing_outputs import (
    create_se_outputs,
    load_main_output,
)
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.utils import read_run_id
from mbs_results.utilities.validation_checks import validate_config


def run_final_outputs(config_user_dict=None):
    config = load_config("config_user.json", config_user_dict)
    config["run_id"] = read_run_id()
    validate_config(config)

    main_mbs_output = load_main_output(config)
    create_se_outputs(main_mbs_output, config)


if __name__ == "__main__":
    run_final_outputs()
