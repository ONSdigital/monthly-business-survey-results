from importlib import metadata

from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.outputs.selective_editing_contributer_output import (
    get_selective_editing_contributer_output,
)
from mbs_results.outputs.selective_editing_question_output import (
    create_selective_editing_question_output,
)
from mbs_results.outputs.turnover_analysis import create_turnover_output
from mbs_results.outputs.weighted_adj_val_time_series import (
    get_weighted_adj_val_time_series,
)


def produce_additional_outputs(config: dict):
    """
    Function to write additional outputs

    Parameters
    ----------
    config : Dict
        main pipeline configuration

    Returns
    -------
    None.
        Outputs are written to output path defined in config

    """

    additional_outputs = get_additional_outputs(
        config,
        {
            "selective_editing_contributor": get_selective_editing_contributer_output,
            "selective_editing_question": create_selective_editing_question_output,
            "turnover_output": create_turnover_output,
            "weighted_adj_val_time_series": get_weighted_adj_val_time_series,
        },
    )

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    for output in additional_outputs:
        filename = f"{output}_v{file_version_mbs}_{snapshot_name}.csv"
        additional_outputs[output].to_csv(config["output_path"] + filename)
