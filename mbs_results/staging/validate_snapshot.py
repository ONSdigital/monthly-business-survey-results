import logging

import pandas as pd

from mbs_results.utilities.outputs import save_df


def validate_snapshot(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    config: dict,
):
    """
    Wrapper to run validate snapshot functions.

    Parameters
    ----------
    responses : pd.DataFrame
        DataFrame containing responses data.
    contributors : pd.DataFrame
        DataFrame containing contributors data.
    config : dict
        Dictionary containing "filter_out_questions" key.
    """

    non_response_in_responses(
        responses=responses,
        contributors=contributors,
        status=config["status"],
        reference=config["reference"],
        period=config["period"],
        non_response_statuses=config["non_response_statuses"],
        config=config,
    )

    check_for_null_target(
        config=config,
        responses=responses,
        target=config["target"],
        question_no=config["question_no"],
    )


def check_for_null_target(
    config: dict,
    responses: pd.DataFrame,
    target: str,
    question_no: str,
) -> pd.DataFrame:
    """
    Validates that there are no empty strings and nulls in the target column of
    responses.

    Parameters
    ----------
    config : dict
        Dictionary containing "filter_out_questions" key.
    responses : pd.DataFrame
        DataFrame containing responses data.
    target : str
        The name of the target column.
    question_no : str
        The name of the question number.

    Raises
    ------
    Warning
        If any empty strings or nulls are found in the target column, a warning
        is logged with details.
    """
    logger = logging.getLogger(__name__)

    # Check for empty strings in the target column
    empty_string_rows = responses[responses[target] == ""]
    if not empty_string_rows.empty:
        first_5 = (
            empty_string_rows[["reference", "period"]]
            .head(5)
            .drop_duplicates()
            .values.tolist()
        )
        logger.warning(
            f"There are {len(empty_string_rows)} rows with empty strings in the "
            f"'{target}' column. The first 5 (or less) (reference, period) groups are: "
            f"{first_5}"
        )

    filter_out_qs = config["filter_out_questions"]
    filtered_responses = responses[~responses[question_no].isin(filter_out_qs)]

    null_rows = filtered_responses[filtered_responses[target].isnull()]
    if not null_rows.empty:
        first_5 = (
            null_rows[["reference", "period"]].head(5).drop_duplicates().values.tolist()
        )
        logger.warning(
            f"There are {len(null_rows)} rows with nulls in the '{target}' column "
            f"after filtering out questions in {filter_out_qs}.  "
            f"The first 5 (or less) (reference, period) groups are: {first_5}"
        )

    output_df = (
        pd.concat([empty_string_rows, null_rows])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    if not output_df.empty:
        save_df(
            output_df,
            "null_or_empty_adjustedresponse.csv",
            config,
            config["debug_mode"],
        )
        if config["debug_mode"]:
            logger.warning(
                "A file containing all rows with a null/empty adjustedresponse"
                "has been produced as check_for_null_target.csv."
            )
    return output_df


def non_response_in_responses(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    status: str,
    reference: str,
    period: str,
    non_response_statuses: list,
    config: dict,
) -> pd.DataFrame:
    """
    Validate that there are no reference and period groupings
    that are listed as non-response statuses in contributors
    but are present in responses.

    Parameters
    ----------
    responses : pd.DataFrame
        DataFrame containing survey responses.
    contributors : pd.DataFrame
        DataFrame containing contributor information.
    status : str
        The name of the column containing the status variable.
    reference : str
        The name of the column containing the reference variable.
    period : str
        The name of the column containing the period (date) variable.
    non_response_statuses : list
        A list of statuses that should be treated as non-responders. These
        should be present in the `status` column of the contributors dataframe.
    config : dict
        The config read in as a dictionary.


    Raises
    ------
    Warning
        If any reference and period groupings are found in both non-response
        statuses in contributors and in responses, a warning is raised with
        details.
    """

    logger = logging.getLogger(__name__)

    non_responses = contributors[contributors[status].isin(non_response_statuses)]

    if len(non_responses) > 0:

        non_responses = non_responses.set_index([reference, period])
        responses = responses.set_index([reference, period])

        common_index = responses.index.intersection(non_responses.index)

        if len(common_index) > 0:
            warning_message = f"""There are {len(common_index)} period and
            reference groupings that are listed as non-response statuses in contributors
            but are present in responses. The first 5 (or less) of these are:
        {common_index[:min(5, len(common_index))].to_list()}.
        The target value for these reference and periods will be set to null."""  # noqa

            logger.warning(warning_message)

            non_response_in_responses = responses.loc[common_index]

            save_df(
                non_response_in_responses.reset_index(),
                "non_response_in_responses.csv",
                config,
                config["debug_mode"],
            )

            if config["debug_mode"]:
                logger.warning(
                    "A file containing all rows with non-response statuses present"
                    "in responses has been produced as non_response_in_responses.csv."
                )
            return non_response_in_responses

    else:
        logger.warning(
            f"""No instances of status {','.join(non_response_statuses)}
                      in the status column in contributors"""
        )
