from mbs_results.outputs.selective_editing_outputs import (
    create_se_outputs,
    load_imputation_output,
)
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import validate_config


def run_final_outputs(config_user_dict=None):
    config = load_config("config_user.json", config_user_dict)
    validate_config(config)

    imputation_output = load_imputation_output(config)
    create_se_outputs(imputation_output, config)


if __name__ == "__main__":
    run_final_outputs()
