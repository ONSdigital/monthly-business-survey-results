import logging
from importlib import metadata

import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import produce_additional_outputs
from mbs_results.staging.back_data import read_back_data
from mbs_results.utilities.inputs import load_config

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="test.txt",
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def back_data_wrapper():

    config = load_config(path="../config.json")

    # Read in back data
    back_data = read_back_data(config)

    # Run apply_imputation_link function to get construction links
    back_data_cons_matches = flag_construction_matches(back_data, **config)
    back_data_imputation = calculate_imputation_link(
        back_data_cons_matches,
        match_col="flag_construction_matches",
        link_col="construction_link",
        predictive_variable=config["auxiliary"],
        **config,
    )

    # Running all of estimation and outliers
    back_data_estimation = estimate(back_data_imputation, config)
    back_data_outliering = detect_outlier(back_data_estimation, config)

    back_data_output = back_data_outliering[
        [
            "period",
            "reference",
            "questioncode",
            "design_weight",
            "frosic2007",
            "formtype",
            "adjusted_value",
            "design_weight",
            "outlier_weight",
            "calibration_factor",
            "frotover",
            "construction_link",
            "response_type",  # In place of imputation marker - is this correct?
        ]
    ]

    # Link to produce_additional_outputs
    produce_additional_outputs(config, back_data_output)

    qa_selective_editing_outputs(config)

    return back_data_outliering


def qa_selective_editing_outputs(config: dict):
    """
    function to QA check the selective editing outputs
    Outputs warings to logging file if any issues are found

    Parameters
    ----------
    config : dict
        main config for pipeline
    """

    # Loading SE outputs, function to create SE outputs cannot return them, easier to
    # load them here

    logger.info("QA checking selective editing outputs")

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    se_contributor_path = (
        config["output_path"]
        + f"selective_editing_contributor_v{file_version_mbs}_{snapshot_name}.csv"
    )
    se_question_path = (
        config["output_path"]
        + f"selective_editing_question_v{file_version_mbs}_{snapshot_name}.csv"
    )

    contributor_df = pd.read_csv(se_contributor_path).rename(
        columns={"ruref": "reference"}
    )
    question_df = pd.read_csv(se_question_path).rename(columns={"ruref": "reference"})

    # Checking that references match
    contributor_unique_reference = contributor_df["reference"].unique().tolist()
    question_unique_reference = question_df["reference"].unique().tolist()
    unmatched_references = list(
        set(contributor_unique_reference).symmetric_difference(
            set(question_unique_reference)
        )
    )

    if len(unmatched_references) > 0:
        logger.warning(
            f"There are {len(unmatched_references)} unmatched refrences in the"
            " contributor and question SE outputs"
            f"unmatched references {unmatched_references}"
        )

    # Checking for duplicates
    groupby_cols = {
        "contributor": ["period", "reference"],
        "question": ["period", "reference", "question_code"],
    }
    dataframe_dict = {"contributor": contributor_df, "question": question_df}
    for dataframe_name in ["contributor", "question"]:
        dataframe = dataframe_dict.get(dataframe_name)
        duplicated = dataframe[
            dataframe.duplicated(subset=groupby_cols[dataframe_name], keep=False)
        ]
        if duplicated.shape[0] > 0:
            logger.warning(
                f"""There are {duplicated.shape[0]}
            duplicated {dataframe_name} in the SE outputs"""
            )
        else:
            logger.info(
                f"no duplicates in {dataframe_name} dataframe columns "
                f"{groupby_cols[dataframe_name]}"
            )

        if dataframe.isnull().sum(axis=0).any():
            null_columns = dataframe.isnull().sum(axis=0)
            null_columns = null_columns[null_columns > 0]
            if not null_columns.empty:
                logger.warning(
                    f"Nulls or NaNs detected in se {dataframe_name} "
                    "dataframe in the following columns:\n"
                    f"{null_columns}"
                )
        else:
            logger.info(f"No nulls or NaNs detected in {dataframe_name} dataframe")

    logger.info("QA of SE outputs finished")
