import logging
from importlib import metadata

from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files
from mbs_results.utilities.inputs import read_csv_wrapper

logger = logging.getLogger(__name__)


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

    contributor_df = read_csv_wrapper(
        se_contributor_path, config["platform"], config["bucket"]
    ).rename(columns={"ruref": "reference"})

    question_df = read_csv_wrapper(
        se_question_path, config["platform"], config["bucket"]
    ).rename(columns={"ruref": "reference"})

    # Checking that references match
    contributor_unique_reference = contributor_df["reference"].tolist()
    question_unique_reference = question_df["reference"].tolist()
    unmatched_references = list(
        set(contributor_unique_reference).symmetric_difference(
            set(question_unique_reference)
        )
    )

    if len(unmatched_references) > 0:
        logger.warning(
            f"There are {len(unmatched_references)} unmatched references in the "
            "contributor and question SE outputs "
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

    finalsel = read_and_combine_colon_sep_files(config)
    finalsel["formtype"] = finalsel["formtype"].astype(int)

    finalsel_unique_reference = set(finalsel["reference"].tolist())

    unmatched_contributor = list(
        finalsel_unique_reference.symmetric_difference(contributor_unique_reference)
    )
    unmatched_question = list(
        finalsel_unique_reference.symmetric_difference(question_unique_reference)
    )

    excluded_formtype = [203, 204]

    mask = finalsel["reference"].isin(unmatched_contributor + unmatched_question)

    excluded = finalsel[mask & finalsel["formtype"].isin(excluded_formtype)]
    missed = finalsel[mask & ~finalsel["formtype"].isin(excluded_formtype)]

    if len(missed) > 0:
        logger.warning(
            f"There are {len(missed)} unmatched references in the "
            "finalsel and SE outputs "
            f"unmatched references {missed['reference'].unique()}"
        )
    if len(excluded) > 0:
        logger.info(
            f"There are {len(excluded)} unmatched references in the "
            "finalsel and SE outputs with water formtypes"
        )

    logger.info("QA of SE outputs finished")
