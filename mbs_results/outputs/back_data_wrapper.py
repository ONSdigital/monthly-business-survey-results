from importlib import metadata

import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import produce_additional_outputs
from mbs_results.staging.back_data import read_back_data
from mbs_results.utilities.inputs import load_config


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

    qa_seletive_editing_outputs(config)

    return back_data_outliering


def qa_seletive_editing_outputs(config: dict):
    """
    function to QA check the selective editing outputs

    Parameters
    ----------
    config : dict
        _description_
    """
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

    contributor_unique_reference = contributor_df["reference"].unique().tolist()
    question_unique_reference = question_df["reference"].unique().tolist()
    unique_references_not_in_con_question = list(
        set(contributor_unique_reference).symmetric_difference(
            set(question_unique_reference)
        )
    )
    print(unique_references_not_in_con_question)

    print(
        len(contributor_unique_reference),
        len(question_unique_reference),
        len(unique_references_not_in_con_question),
    )

    print()
    duplicated_references = contributor_df[contributor_df.duplicated(keep=False)]
    print(duplicated_references)
    dupes = question_df.drop_duplicates(keep=False)
    print(dupes.shape)

    question_df.groupby(["period", "reference", "question_code"]).apply(
        lambda df: print(df) if df["survey_code"].count() > 1 else None
    )
    print("duplicated questions ^")
    contributor_df.groupby(["period", "reference"]).apply(
        lambda df: print(df) if df["survey_code"].count() > 1 else None
    )
    print("duplicated con ^")

    print(len(question_df["reference"].unique()))
    print(
        "unique in con:",
        len(contributor_df["reference"].unique()),
        "shape of full con: (should match)",
        contributor_df.shape,
    )
    print(
        "unique in ques:",
        len(question_df["reference"].unique()),
        "shape of full question: (shouldn't match)",
        question_df.shape,
    )
    print("Nulls or NaNs in each row of se_contributor:")
    print(contributor_df.isnull().sum(axis=0))

    print("Nulls or NaNs in each row of se_question:")
    print(question_df.isnull().sum(axis=0))
