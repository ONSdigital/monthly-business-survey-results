import logging
import os
import warnings
from importlib import metadata

import pandas as pd

from mbs_results.utilities.utils import append_filter_out_questions

logger = logging.getLogger(__name__)


def validate_config(config):
    """
    Run validation checks on the main config dictionary

    Parameters
    ----------
    config: Dict
      main pipeline configuration

    """

    if colnames_clash(**config):
        raise ValueError(
            """Overlapping column names in responses_keep_cols and
            contributors_keep_cols (main config)."""
        )
    if period_and_reference_not_given(**config):
        raise ValueError(
            """Period and/or Reference is not given in responses_keep_cols
             and/or contributors_keep_cols (main config). """
        )


def colnames_clash(
    reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Check for overlapping columns between the input data to be kept,
    except for id variables

    Parameters
    ----------
    reference: Str
      the name of the reference column
    period: Str
      the name of the period column
    response_keep_cols: Str
      the names of the columns to keep from the responses data
    contributors_keep_cols: Str
        the names of the columns to keep from the contributors data
    **config: Dict
      main pipeline configuration. Can be used to input the entire config dictionary
    Returns
    -------
    bool
      Returns true if any column names are in both contributors and responses,
      excluding period and reference.
      False otherwise
    """

    return any(
        [
            column in contributors_keep_cols and column not in [reference, period]
            for column in responses_keep_cols
        ]
    )


def period_and_reference_not_given(
    reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Function checks that both reference and period columns are supplied in
    response_keep_cols and contributors_keep_cols.
    Returns True if period or reference is missing from either
    responses_keep_cols and contributors_keep_cols, False otherwise

    reference: Str
      the name of the reference column
    period: Str
      the name of the period column
    response_keep_cols: Str
      the names of the columns to keep from the responses data
    contributors_keep_cols: Str
        the names of the columns to keep from the contributors data
    **config: Dict
      main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    bool
        Returns True if response and period are not in both responses_keep_cols
        and contributors_keep_cols
        False otherwise
    """

    return any(
        [
            column
            for column in [reference, period]
            if (column not in responses_keep_cols)
            or (column not in contributors_keep_cols)
        ]
    )


def validate_indices(responses, contributors):
    """
    Check that all indices (reference x period) match
    across the responses and contributors dataframes.

    Raises an error if there are any mismatches.

    Parameters
    ----------
    responses: pd.DataFrame
      the responses dataset with index variables set
    contributors: pd.DataFrame
      the contributors dataset with index variables set

    """

    diff = set(responses.index.unique()) ^ set(contributors.index.unique())

    if len(diff) > 0:
        non_matches = "".join([f"reference: {i[0]}, period: {i[1]}\n" for i in diff])
        raise ValueError(
            f"""There are mismatched records between the responses and
            contributors datasets:\n {non_matches}"""
        )


def validate_config_datatype_input(
    responses_keep_cols, contributors_keep_cols, **config
):
    """
    function to validate config datatypes inputted into the config file

    Parameters
    ----------
    responses_keep_cols : dict
        dictionary containing columns to keep from responses and datatypes
    contributors_keep_cols : _type_
        dictionary containing columns to keep from contributors and datatypes

    Raises
    ------
    ValueError
        ValueError if the specified datatype is not in the accepted_type list
        ints and floats do not need to specify number of bits i.e. int32 etc.
    """
    joint_dict = {**responses_keep_cols, **contributors_keep_cols}
    accepted_types = ["str", "float", "int", "date", "bool", "category"]
    incorrect_datatype = [
        item for item in list(joint_dict) if joint_dict.get(item) not in accepted_types
    ]

    if incorrect_datatype:
        given_types = [joint_dict.get(key) for key in incorrect_datatype]
        raise ValueError(
            """Check the inputted datatype(s) for column(s) {}:{},
            only the following datatypes are accepted: {}""".format(
                incorrect_datatype, given_types, accepted_types
            )
        )


def validate_config_repeated_datatypes(
    responses_keep_cols, contributors_keep_cols, **config
):
    """
    Checking that repeated columns do not have conflicting data types

    Parameters
    ----------
    responses_keep_cols : dict
        dictionary containing columns to keep from responses and datatypes
    contributors_keep_cols : _type_
        dictionary containing columns to keep from contributors and datatypes

    Raises
    ------
    ValueError
        ValueError if any repeated column has a different data type in the contributors
        and responses data sets.
    """

    mismatched_types = [
        key
        for key in responses_keep_cols
        if (key in contributors_keep_cols)
        and (responses_keep_cols[key] != contributors_keep_cols[key])
    ]
    if mismatched_types:
        # Warning to catch if the same column name has different types
        raise ValueError(
            "Mismatched data types between two dictionaries in columns: {}".format(
                mismatched_types
            )
        )


def validate_manual_constructions(df, manual_constructions):
    """
    Checks that manual construction identifiers match those in the main dataset

    Parameters
    ----------
    df: pd.DataFrame
        the main dataframe after preprocessing
    manual_constructions: pd.DataFrame
        the manual constructions input read in as a dataframe

    Raises
    ------
    ValueError
        ValueError if any combinations of period and reference appear in the manual
        constructions input but not in the main dataframe
    """

    incorrect_ids = set(manual_constructions.index) - set(df.index)

    if len(incorrect_ids) > 1:
        string_ids = " ".join(
            [f"\nreference: {str(i[0])}, period: {str(i[1])}" for i in incorrect_ids]
        )

        raise ValueError(
            f"""There are reference and period combinations in the manual constructions
      with no match: {string_ids}"""
        )


def validate_staging(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post staging")


def validate_imputation(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post imputation")
    output_path = config["output_path"]
    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    imputation_filename = f"imputation_output_v{file_version_mbs}_{snapshot_name}.csv"
    df.to_csv(output_path + imputation_filename, index=False)


def validate_estimation(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post estimation")
    output_path = config["output_path"]
    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    estimate_filename = f"estimation_output_v{file_version_mbs}_{snapshot_name}.csv"
    df.to_csv(output_path + estimate_filename, index=False)


def validate_outlier_detection(df: pd.DataFrame, config: dict):
    warnings.warn("A placeholder function for validating dataframe post outliering")
    output_path = config["output_path"]
    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = config["mbs_file_name"].split(".")[0]
    outlier_filename = f"outlier_output_v{file_version_mbs}_{snapshot_name}.csv"

    # Must be same as save_full_path argument of filter_out_questions() (in staging)
    # Path must be full path containing directory and file name
    # e.g. folder1/folder2/snapsot_filter_out_questions.csv

    snapshot_name = os.path.splitext(config["mbs_file_name"])[0]

    filtered_questions_path = (
        config["output_path"] + snapshot_name + "filter_out_questions.csv"
    )

    df = append_filter_out_questions(df, filtered_questions_path)
    df.to_csv(output_path + outlier_filename, index=False)


def qa_selective_editing_outputs(config: dict):
    """
    function to QA check the selective editing outputs
    Outputs warnings to logging file if any issues are found

    Parameters
    ----------
    config : dict
        main config for pipeline
    """

    # Loading SE outputs, function to create SE outputs cannot return them, easier to
    # load them here

    logger.info("QA checking selective editing outputs")

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    period = config["period_selected"]
    se_contributor_path = (
        config["output_path"] + f"secontributors009_{period}_v{file_version_mbs}.csv"
    )
    se_question_path = (
        config["output_path"] + f"sequestions009_{period}_v{file_version_mbs}.csv"
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
            f"There are {len(unmatched_references)} unmatched references in the"
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
            logger.warning(duplicated)
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
