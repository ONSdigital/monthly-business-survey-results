import os
from importlib import metadata

import pandas as pd

from mbs_results import logger
from mbs_results.outputs.csdb_output import create_csdb_output
from mbs_results.outputs.get_additional_outputs import get_additional_outputs
from mbs_results.outputs.growth_rates_output import get_growth_rates_output
from mbs_results.outputs.ocea_srs_outputs import produce_ocea_srs_outputs
from mbs_results.outputs.pivot_imputation_value import create_imputation_link_output
from mbs_results.outputs.qa_output import produce_qa_output
from mbs_results.outputs.scottish_welsh_gov_outputs import generate_devolved_outputs
from mbs_results.outputs.selective_editing_contributer_output import (
    get_selective_editing_contributor_output,
)
from mbs_results.outputs.selective_editing_question_output import (
    create_selective_editing_question_output,
)
from mbs_results.outputs.turnover_analysis import create_turnover_output
from mbs_results.utilities.utils import (
    append_filter_out_questions,
    get_versioned_filename,
)


def get_additional_outputs_df(
    estimation_output: pd.DataFrame, outlier_output: pd.DataFrame, config: dict
):
    """
    Creating dataframe that contains all variables needed for producing additional
    outputs.

    Parameters
    ----------
    estimation_output : pd.DataFrame
        Dataframe output from the estimation stage of the pipeline
    outlier_output : pd.DataFrame
        Dataframe output from the outliering stage of the pipeline

    Returns
    -------
    pd.DataFrame

    """

    snapshot_name = os.path.basename(config["snapshot_file_path"]).split(".")[0]

    filtered_questions_path = (
        config["output_path"] + snapshot_name + "_filter_out_questions.csv"
    )
    outlier_output = append_filter_out_questions(
        outlier_output, filtered_questions_path
    )

    additional_outputs_df = outlier_output

    return additional_outputs_df


def produce_additional_outputs(config: dict, additional_outputs_df: pd.DataFrame):
    """
    Function to write additional outputs

    Parameters
    ----------
    config : Dict
        main pipeline configuration
    additional_outputs_df : pd.DataFrame
        Dataframe to feed in as arguments for additional outputs

    Returns
    -------
    None.
        Outputs are written to output path defined in config

    """

    additional_outputs = get_additional_outputs(
        config,
        {
            "turnover_output": create_turnover_output,
            "growth_rates_output": get_growth_rates_output,
            "produce_ocea_srs_outputs": produce_ocea_srs_outputs,
            "create_imputation_link_output": create_imputation_link_output,
            "create_csdb_output": create_csdb_output,
            "generate_devolved_outputs": generate_devolved_outputs,
            "produce_qa_output": produce_qa_output,
        },
        additional_outputs_df,
    )

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    for output, (df, name) in additional_outputs.items():
        if name:
            filename = name
        else:
            filename = get_versioned_filename(output, config)
        # output_value = additional_outputs[output]
        if isinstance(df, dict):
            # if the output is a dictionary (e.g. from generate_devolved_outputs),
            # we need to save each DataFrame in the dictionary
            for nation, df in df.items():
                nation_filename = f"{config['output_path']}{nation.lower()}_{filename}"
                df.to_csv(nation_filename, index=False)
                logger.info(nation_filename + " saved")
        else:
            # if the output is a DataFrame, save it directly
            df.to_csv(config["output_path"] + filename, index=False)
            logger.info(config["output_path"] + filename + " saved")


def produce_selective_editing_outputs(
    config: dict, additional_outputs_df: pd.DataFrame
):
    """
    Function to write selective editing outputs

    Parameters
    ----------
    config : Dict
        main pipeline configuration
    additional_outputs_df : pd.DataFrame
        Dataframe to feed in as arguments for additional outputs

    Returns
    -------
    None.
        Outputs are written to output path defined in config

    """

    additional_outputs = get_additional_outputs(
        config,
        {
            "selective_editing_contributors": get_selective_editing_contributor_output,
            "selective_editing_questions": create_selective_editing_question_output,
        },
        additional_outputs_df,
        selective_editing=True,
    )

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]

    # Stop function if no additional_outputs are listed in config.
    if additional_outputs is None:
        return

    for output, (df, name) in additional_outputs.items():
        if name:
            filename = name
            print("if name")
        else:
            file = output.split("_")[-1]
            period = df["period"].unique()[0].astype(int)
            filename = f"se{file}009_{period}_v{file_version_mbs}.csv"
        # output_value = additional_outputs[output]
        if isinstance(df, dict):
            # if the output is a dictionary (e.g. from generate_devolved_outputs),
            # we need to save each DataFrame in the dictionary
            for nation, df in df.items():
                nation_filename = f"{config['output_path']}{nation.lower()}_{filename}"
                df.to_csv(nation_filename, index=False)
                logger.info(nation_filename + " saved")
        else:
            # if the output is a DataFrame, save it directly
            df.to_csv(config["output_path"] + filename, index=False)
            logger.info(config["output_path"] + filename + " saved")
