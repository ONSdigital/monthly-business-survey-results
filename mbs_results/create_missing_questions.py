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
    Adds missing questions to responses data by comparing dataframes.
    Reference, period, formid and question_no uniquely identify rows in responses data.
    Reference, period and formid to uniquely identify rows in contributors data.
    Uses mapper to create all expected questions which should be present in the
    responses dataframe.


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
    Responses datafame with missing questions.

    """

    actual_responses_df = responses_df.filter([reference, period, formid, question_no])

    expected_responses = (
        contributors_df.filter([reference, period, formid])  # Select needed fields
        .assign(
            **{question_no: contributors_df[formid].map(mapper)}
        )  # Create new column with list of questions as value
        .loc[lambda df: df[question_no].str.len() > 0]
        .explode(question_no)  # Convert questions to rows
    )

    anti_join_df = expected_responses.merge(
        actual_responses_df, how="left", indicator=True
    )
    anti_join_df = anti_join_df[anti_join_df["_merge"] == "left_only"].drop(
        columns="_merge"
    )

    concatenated_responses = pd.concat([responses_df, anti_join_df], ignore_index=True)

    return concatenated_responses
