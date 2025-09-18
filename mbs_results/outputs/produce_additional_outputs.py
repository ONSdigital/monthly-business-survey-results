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
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.pounds_thousands import create_pounds_thousands_column
from mbs_results.utilities.utils import get_versioned_filename


def get_additional_outputs_df(
    df: pd.DataFrame, unprocessed_data: pd.DataFrame, config: dict
):
    """
    Creating dataframe that contains all variables needed for producing additional
    outputs.
    Create adjustedresponse_pounds_thousands column based on question numbers in config.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe output from the outliering stage of the pipeline
    unprocessed_data : pd.DataFrame
        Dataframe with all question codes which weren't processed through
        mbs methods like qcode 11, 12, 146.
    config : dict
        main pipeline configuration.

    Returns
    -------
    pd.DataFrame

    """
    questions_to_apply = config.get("pounds_thousands_questions")
    question_col = config.get("question_no")
    dest_col = config.get("pound_thousand_col")
    target = config.get("target")

    # below needed for mandotary and optional outputs
    final_cols = [
        config["reference"],
        config["period"],
        config["sic"],
        "classification",
        config["cell_number"],
        config["auxiliary"],
        "froempment",
        "formtype",
        "imputed_and_derived_flag",
        question_col,
        config["status"],
        "design_weight",
        config["calibration_factor"],
        "outlier_weight",
        f"imputation_flags_{target}",
        "imputation_class",
        f"f_link_{target}",
        f"default_link_f_match_{target}",
        f"b_link_{target}",
        f"default_link_b_match_{target}",
        "construction_link",
        "flag_construction_matches_count",
        "default_link_flag_construction_matches",
        "constrain_marker",
        target,
        "response",
        "status",
    ]
    if not config["filter"]:
        count_variables = [f"b_match_{target}_count", f"f_match_{target}_count"]
    else:
        count_variables = [
            f"b_match_filtered_{target}_count",
            f"f_match_filtered_{target}_count",
        ]

    final_cols += count_variables

    df = create_pounds_thousands_column(
        df,
        question_col=question_col,
        source_col=target,
        dest_col=dest_col,
        questions_to_apply=questions_to_apply,
        ensure_at_end=True,
    )

    # converting cell_number to int
    # needed for outputs that use cell_number for sizebands

    df = df.astype({"classification": str, config["cell_number"]: int})

    df = df[final_cols]

    df = pd.concat([df, unprocessed_data])

    df.reset_index(drop=True, inplace=True)

    return df


def produce_additional_outputs(
    additional_outputs_df: pd.DataFrame,
    qa_outputs: bool,
    optional_outputs: bool,
    config: dict,
):
    """
    Produces additional outputs.

    mandatory outputs are defined in config['mandatory_outputs'] and will
    be created when mandatory_outputs arg is set to TRUE

    optional_outputs are all the available outputs apart from the ones
    define in config['mandatory_outputs']


    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        Post methods dataframe.
    qa_outputs : bool
        Whether to produce mandotaty for QA.
    additional_outputs : bool
        Whether to produce any non mandotaty outputs.
    config : dict
        main pipeline configuration.

    Returns
    -------
    None.

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
        qa_outputs,
        optional_outputs,
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
                write_csv_wrapper(
                    df,
                    nation_filename,
                    config["platform"],
                    config["bucket"],
                    index=False,
                )

                logger.info(nation_filename + " saved")
        else:
            # if the output is a DataFrame, save it directly
            write_csv_wrapper(
                df,
                config["output_path"] + filename,
                config["platform"],
                config["bucket"],
                index=False,
            )
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
        qa_outputs=False,
        optional_outputs=False,
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
                write_csv_wrapper(
                    df,
                    nation_filename,
                    config["platform"],
                    config["bucket"],
                    index=False,
                )

                logger.info(nation_filename + " saved")
        else:
            # if the output is a DataFrame, save it directly
            write_csv_wrapper(
                df,
                config["output_path"] + filename,
                config["platform"],
                config["bucket"],
                index=False,
            )
            logger.info(config["output_path"] + filename + " saved")
