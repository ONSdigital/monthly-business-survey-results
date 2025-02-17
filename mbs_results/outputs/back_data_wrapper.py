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
    level=logging.DEBUG,
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
            f"There are {unmatched_references} unmatched refrences in the"
            + " contributor and question SE outputs"
        )
        logger.warning(f"unmatched references {unmatched_references}")

    # Checking for duplicates in contributors
    duplicated_contributors = contributor_df[
        contributor_df.duplicated(subset=["period", "reference"], keep=False)
    ]

    if duplicated_contributors.shape[0] > 0:
        logger.warning(
            f"""There are {duplicated_contributors.shape[0]}
            duplicated contributors in the SE outputs"""
        )
        logger.warning(duplicated_contributors)

    # Duplicates are expected in questions file if reference has q40 and 49 on same form
    # Hence why we also group by question code in this case
    duplicated_questions = question_df[
        question_df.duplicated(
            subset=["period", "reference", "question_code"], keep=False
        )
    ]

    if duplicated_questions.shape[0] > 0:
        logger.warning(
            f"There are {duplicated_questions.shape[0]} "
            "duplicated contributors in the SE outputs"
        )
        logger.warning(duplicated_questions)

    # Checking for nulls in contributors (expecting one null)
    if contributor_df.isnull().sum(axis=0).any():
        null_contributor_columns = contributor_df.isnull().sum(axis=0)
        null_contributor_columns = null_contributor_columns[
            null_contributor_columns > 0
        ]
        if not null_contributor_columns.empty:
            logger.warning(
                "Nulls or NaNs detected in se contributor "
                "dataframe in the following columns:\n"
                f"{null_contributor_columns}"
            )
            logger.warning(f"\n{null_contributor_columns}")

    # Checking for nulls in contributors (expecting one null)
    # Expect lot more nulls in imputation_marker as derived is left blank
    if question_df.isnull().sum(axis=0).any():
        null_question_columns = question_df.isnull().sum(axis=0)
        null_question_columns = null_question_columns[null_question_columns > 0]
        if not null_question_columns.empty:
            logger.warning(
                "Nulls or NaNs detected in se question "
                "dataframe in the following columns:\n"
                f"{null_question_columns}"
            )
            logger.warning(
                "Null or NaNs detected in the following columns:\n"
                f"{null_question_columns}"
            )
