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
    # TODO: This has already been combined somewhere updstream
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
    imputation_class: str,
    a_weight: str,
    o_weight: str,
    g_weight: str,
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
    imputation_class : str
        name of column in dataframe containing imputation class variable
    a_weight : str
        name of column in dataframe containing a_weight variable
    o_weight : str
        name of column in dataframe containing o_weight variable
    g_weight : str
        name of column in dataframe containing g_weight variable
    calc_period : int
        period to take the weights for estimation of standardising factor in
        the format yyyymm

    Returns
    -------
    pd.DataFrame
        dataframe with standardising factor estimated and summed by domain for
        each reference.

    """
    questions_selected = [40, 49]
    current_df = dataframe[(dataframe[period] == period_selected)]
    current_df = current_df[current_df[question_no].isin(questions_selected)]

    # The standardising factor is created for each record before summing for each
    # domain-question grouping.
    current_df["unit_standardising_factor"] = (
        current_df[predicted_value]
        * current_df[a_weight]
        * current_df[o_weight]
        * current_df[g_weight]
    )

    current_df["standardising_factor"] = current_df.groupby([domain, question_no])[
        "unit_standardising_factor"
    ].transform("sum")

    output_df = current_df[
        [
            period,
            reference,
            question_no,
            "standardising_factor",
            predicted_value,
            imputation_marker,
            imputation_class,
        ]
    ]

    return output_df.reset_index(drop=True)


def calculate_auxiliary_value(
    dataframe: pd.DataFrame,
    reference: str,
    period: str,
    question_no: str,
    frozen_turnover: str,
    construction_link: str,
    imputation_class: str,
    period_selected: int,
) -> pd.DataFrame:
    """
    Returning auxiliary values for questions 40 and 49.
    Auxiliary is frozen turnover for Q40 and
    construction link * frozen turnover for Q49.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe
    reference : str
        name of column in dataframe containing reference variable
    period : str
        name of column in dataframe containing period variable
    question_no : str
        name of column in dataframe containing question code variable
    frozen_turnover : str
        name of column in dataframe containing auxiliary value variable
    construction_link : str
        name of column in dataframe containing construction link variable
    imputation_class : str
        name of column in dataframe containing imputation class, where
        there is one contruction link per imputation_class and period
    calc_period : int
        period to take the weights for estimation of standardising factor in
        the format yyyymm

    Returns
    -------
    pd.DataFrame
        dataframe with calculated auxiliary_value and reference, period
        and question_no in order to merge back onto selective editing
        questions output

    """

    current_df = dataframe[(dataframe[period] == period_selected)]

    q40 = current_df[current_df[question_no] == 40]
    q49 = current_df[current_df[question_no] == 49]

    q40["auxiliary_value"] = q40[frozen_turnover]
    q49["auxiliary_value"] = q49[frozen_turnover] * q49[construction_link]

    keep_cols = [reference, period, question_no, "auxiliary_value", imputation_class]

    output_df = pd.concat([q40[keep_cols], q49[keep_cols]])

    return output_df.reset_index(drop=True)
