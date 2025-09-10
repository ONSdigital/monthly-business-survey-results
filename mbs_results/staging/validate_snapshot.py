import logging
import warnings

import pandas as pd


def validate_snapshot(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    status: str,
    reference: str,
    period: str,
    non_response_statuses: list,
):
    """
    Validate that there are no reference and period groupings that are listed as
    non-response statuses in contributors but are present in responses.

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


    Raises
    ------
    Warning
        If any reference and period groupings are found in both non-response statuses
        in contributors and in responses, a warning is raised with details.
    """

    logger = logging.getLogger(__name__)

    non_responses = contributors[contributors[status].isin(non_response_statuses)]

    if len(non_responses) > 0:

        non_responses = non_responses.set_index([reference, period])
        responses = responses.set_index([reference, period])

        non_response_in_responses = responses.index.intersection(non_responses.index)

        if len(non_response_in_responses) > 0:
            warning_message = f"""There are {len(non_response_in_responses)} period and
            reference groupings that are listed as non-response statuses in contributors
            but are present in responses. The first 5 (or less) of these are:
        {non_response_in_responses[:min(5, len(non_response_in_responses))].to_list()}"""  # noqa

            logger.warning(warning_message)

    else:
        warnings.warn(
            f"""No instances of status {','.join(non_response_statuses)}
                      in the status column in contributors"""
        )
