import pandas as pd


def create_standardising_factor(
    dataframe: pd.DataFrame,
    reference: str,
    period: str,
    domain: str,
    question_code: str,
    predicted_value: str,
    imputation_marker: str,
    a_weight: str,
    o_weight: str,
    g_weight: str,
    auxiliary_value: str,
    previous_period: int,
) -> pd.DataFrame:
    """
    Returning standardising factor summed by domain for questions 40 and 49.
    Standardising factor estimated using a_weights, o_weights and g_weights.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe with domain, a_weights, o_weights, and g_weights
    reference : str
        name of column in dataframe containing reference variable
    period : str
        name of column in dataframe containing period variable
    domain : str
        name of column in dataframe containing domain variable
    question_code : str
        name of column in dataframe containing question code variable
    predicted_value : str
        name of column in dataframe containing predicted value variable
    imputation_marker : str
        name of column in dataframe containing imputation marker variable
    a_weight : str
        name of column in dataframe containing a_weight variable
    o_weight : str
        name of column in dataframe containing o_weight variable
    g_weight : str
        name of column in dataframe containing g_weight variable
    auxiliary_value : str
        name of column in dataframe containing auxiliary value variable
    previous_period : int
        previous period to take the weights for estimation of standardising factor in
        the format yyyymm

    Returns
    -------
    pd.DataFrame
        dataframe with standardising factor estimated and summed by domain for
        each reference.

    """
    previous_df = dataframe[(dataframe[period] == previous_period)]
    previous_df = previous_df[previous_df[question_code].isin([40, 49])]

    previous_df["standardising_factor"] = (
        previous_df[predicted_value]
        * previous_df[a_weight]
        * previous_df[o_weight]
        * previous_df[g_weight]
    )

    previous_df = previous_df.assign(
        standardising_factor=lambda x: x.groupby([domain, question_code]).transform(
            "sum"
        )["standardising_factor"]
    ).astype({"standardising_factor": "float"})

    output_df = previous_df[
        [
            period,
            reference,
            question_code,
            "standardising_factor",
            predicted_value,
            imputation_marker,
            auxiliary_value,
        ]
    ]

    return output_df.reset_index(drop=True)
