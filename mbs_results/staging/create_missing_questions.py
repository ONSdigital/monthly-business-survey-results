import pandas as pd


def create_missing_questions(
    contributors_df: pd.DataFrame,
    responses_df: pd.DataFrame,
    reference: str,
    period: str,
    formid: str,
    question_no: str,
    mapper: dict,
) -> pd.DataFrame:
    """
    Adds missing questions to responses data by checking which question numbers
    are expected in contributors_df.

    A mapper dictionary needs to be passed which maps form id with expected
    question numbers, the function will then check which question numbers
    (per reference,period) exist in responses_df and will add empty rows
    if it does not exist.

    Note:

    Reference, period and question_no uniquely identify rows in responses data.
    Reference, period and formid to uniquely identify rows in contributors data.

    Parameters
    ----------
    contributors_df : pd.DataFrame
        DataFrame containing contributor data for MBS from SPP Snapshot file.
    responders_df : pd.DataFrame
        DataFrame containing response data for MBS from SPP Snapshot file.
    reference : str
        Column name of unique Identifier.
    period : str
        Name of column containing time period.
    formid : str
        Name of column containing form ID.
    question_no : str
        Name of column containing question number.
    mapper : dict
        Contains all expected combinations of form ID values and question number values.

    Returns
    -------
    Responses dataframe with missing questions.

    """

    question_no_in_responses = responses_df.filter([reference, period, question_no])

    expected_question_no = (
        contributors_df.filter([reference, period, formid])  # Select needed fields
        .assign(
            **{question_no: contributors_df[formid].map(mapper)}
        )  # Create new column with list of questions as value
        .loc[lambda df: df[question_no].str.len() > 0]
        .explode(question_no)  # Convert questions to rows
    )

    joined_question_no = expected_question_no.merge(
        question_no_in_responses,
        how="left",
        on=[reference, period, question_no],
        indicator=True,
    )

    missing_responses = joined_question_no[
        joined_question_no["_merge"] == "left_only"
    ].drop(columns="_merge")

    concatenated_responses = pd.concat(
        [responses_df, missing_responses], ignore_index=True
    )

    return concatenated_responses
