import numpy as np
import pandas as pd


def calculate_predicted_value(
    dataframe: pd.DataFrame,
    adjusted_value: str,
    imputed_value: str,
) -> pd.DataFrame:
    """
    Calculate predicted value given a data frame with adjusted and imputed value.
    Predicted value is defined as adjusted_value if adjusted_value is not NaN.
    If adjusted_value is NaN then predicted value is imputed_value.

    Parameters
      ----------
      dataframe : pd.DataFrame
          Dataframe with adjusted_value and imputed_value
      adjusted_value : str
          name of column in dataframe containing adjusted_value variable
      imputed_value : str
          name of column in dataframe containing imputed_value variable

      Returns
      -------
      pd.DataFrame
          dataframe containing predicted_value column, calculated from adjusted_value
          and imputed_value

    """

    dataframe["predicted_value"] = np.where(
        dataframe[adjusted_value].isna(),
        dataframe[imputed_value],
        dataframe[adjusted_value],
    )

    return dataframe


def create_standardising_factor(
    dataframe: pd.DataFrame,
    reference: str,
    period: str,
    domain: str,
    question_no: str,
    predicted_value: str,
    imputation_marker: str,
    a_weight: str,
    o_weight: str,
    g_weight: str,
    auxiliary_value: str,
    period_selected: int,
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
    questions_selected = [40, 49]
    previous_df = dataframe[(dataframe[period] == period_selected)]
    previous_df = previous_df[previous_df[question_no].isin(questions_selected)]
    # The standardising factor is created for each record before summing for each
    # domain-question grouping.
    previous_df["unit_standardising_factor"] = (
        previous_df[predicted_value]
        * previous_df[a_weight]
        * previous_df[o_weight]
        * previous_df[g_weight]
    )

    previous_df["standardising_factor"] = previous_df.groupby([domain, question_no])[
        "unit_standardising_factor"
    ].transform("sum")

    output_df = previous_df[
        [
            period,
            reference,
            question_no,
            "standardising_factor",
            predicted_value,
            imputation_marker,
            auxiliary_value,
        ]
    ]

    return output_df.reset_index(drop=True)
