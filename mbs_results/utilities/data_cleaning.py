from typing import List

import numpy as np
import pandas as pd

from mbs_results.unsorted.validation_checks import (
    validate_indices,
    validate_manual_constructions,
)
from mbs_results.utilities.utils import convert_column_to_datetime


def filter_responses(df, reference, period, last_update):
    """
    Filter the responses data to return only the most recently updated
    row for each reference and period combination

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing responses data from SPP

    reference : str
        Column name containing business reference id.
    period : str
        Column name for survey period.
    last_update : str
        Column name containing auxiliary data.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame of one reference period combination
    """
    return df.sort_values(last_update).groupby([reference, period]).tail(1)


def clean_and_merge(
    snapshot, reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Clean and merge JSON inputs.

    Parameters
    ----------
    snapshot: Dict
      raw input as loaded by research_and_development.utils.hdfs_load_json
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
    pd.DataFrame
      Main analysis dataset

    """
    contributors = pd.DataFrame(snapshot["contributors"])
    responses = pd.DataFrame(snapshot["responses"])

    responses = filter_responses(responses, reference, period, "lastupdateddate")
    responses = responses[responses_keep_cols].set_index([reference, period])
    contributors = contributors[contributors_keep_cols].set_index([reference, period])

    validate_indices(responses, contributors)
    return responses.merge(contributors, on=[reference, period])


def enforce_datatypes(
    df,
    reference,
    period,
    responses_keep_cols,
    contributors_keep_cols,
    temporarily_remove_cols,
    **config
):
    """
    function to change datatypes of columns based on config file

    Parameters
    ----------
    df : pd.DataFrame
        dataframe with combined responses and contributors columns
        index is expected to be 'period' and 'reference'
    responses_keep_cols : dict
        dictionary containing response columns to keep and datatypes
    contributors_keep_cols : dict
        dictionary containing contributors columns to keep and datatypes
    temporarily_remove_cols : list
        list containing column names to drop.
        Implemented to remove columns while not removing datatypes

    Returns
    -------
    pd.DataFrame
        dataframe with correctly formatted column datatypes.
    """
    # Resetting to easily change datatypes
    response_dict = responses_keep_cols
    contributors_dict = contributors_keep_cols

    df_convert = df.copy()
    df_convert = df_convert.reset_index()
    if "index" in df_convert.columns.tolist():
        df_convert.drop("index", axis=1, inplace=True)

    # Joining Dicts will overwrite first dict if values are different.
    # check for this is carried out in 'validate_config_repeated_datatypes'
    joint_dictionary = {
        **response_dict,
        **contributors_dict,
    }

    try:
        temp_remove_cols = temporarily_remove_cols
    except KeyError:
        # deals with case when key is not found in dict
        # Or when temporarily_remove_cols is empty in config
        temp_remove_cols = []

    df_convert.drop(temp_remove_cols, axis=1, inplace=True)
    for key1 in temp_remove_cols:
        # Deletes key and value for any column in temp_remove_cols
        # None handles cases when key is not included in joint_dict
        joint_dictionary.pop(key1, None)

    for key in joint_dictionary:
        type_from_dict = joint_dictionary[key]
        if type_from_dict in ["str", "float", "int", "bool", "category"]:
            df_convert[key] = df_convert[key].astype(type_from_dict)
        elif type_from_dict == "date":
            df_convert[key] = convert_column_to_datetime(df_convert[key])
    # Re-set the index back to reference and period
    df_convert = df_convert.set_index([reference, period])
    return df_convert


def load_manual_constructions(
    df, reference, period, manual_constructions_path, **config
):
    """
    Loads manual construction data from given file path
    performs validation checks and joins this to main dataframe

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with combined responses and contributors columns
        index is expected to be 'period' and 'reference'
    reference: str
        the name of the reference column
    period: str
        the name of the period column
    manual_constructions_path: str
        the location of the manual construction data
    **config: Dict
        main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        dataframe with correctly formatted column datatypes.
    """
    manual_constructions = pd.read_csv(manual_constructions_path)
    manual_constructions[period] = convert_column_to_datetime(
        manual_constructions[period]
    )
    manual_constructions[reference] = manual_constructions[reference].astype("str")
    manual_constructions.set_index([reference, period], inplace=True)

    validate_manual_constructions(df, manual_constructions)

    return df.merge(
        manual_constructions, on=[reference, period], how="outer", suffixes=("", "_man")
    )


def join_manual_constructions(
    df: pd.DataFrame,
    manual_constructions: pd.DataFrame,
    reference: str,
    period: str,
    question_no: str = "question_no",
    **config
):
    """
    joins manual construction data from onto main dataframe
    performs validation checks and converts datatypes of manual
    construction dataframe to same d-types as main dataframe

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with combined responses and contributors columns
        index is expected to be 'period' and 'reference'
    manual_constructions_path: str
        the manual construction dataframe
    reference: str
        the name of the reference column
    period: str
        the name of the period column
    period: str
        the name of the question number column
    **config: Dict
        main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        dataframe with correctly formatted column datatypes.
    """

    question_no_from_df = df[question_no].unique().tolist()
    manual_constructions_filter = manual_constructions.loc[
        manual_constructions[question_no].isin(question_no_from_df)
    ]

    if manual_constructions_filter.empty:
        # return original df as nothing present to use
        # as manual construction
        return df
    else:
        manual_constructions_filter.drop(columns=[question_no], inplace=True)
        if period not in df.columns or reference not in df.columns:
            df = df.reset_index()

        if not is_same_dtype(df, manual_constructions_filter, period):
            manual_constructions_filter[period] = convert_column_to_datetime(
                manual_constructions_filter[period]
            )

        if not is_same_dtype(df, manual_constructions_filter, reference):
            manual_constructions_filter[reference] = manual_constructions_filter[
                reference
            ].astype(df[reference].dtype)

        manual_constructions_filter.set_index([reference, period], inplace=True)
        df.set_index([reference, period], inplace=True)

        validate_manual_constructions(df, manual_constructions_filter)

        return df.merge(
            manual_constructions_filter,
            on=[reference, period],
            how="left",
            suffixes=("", "_man"),
        ).reset_index()


def is_same_dtype(df: pd.DataFrame, df2: pd.DataFrame, col_name: str) -> bool:
    """
    checks if given column in two dataframes have same datatype

    Parameters
    ----------
    df : pd.DataFrame
        dataframe one to compare against
    df2 : pd.DataFrame
        dataframe two to compare against
    col_name : str
        column name to compare datatypes of

    Returns
    -------
    bool
        True if col_name in df and df2 have same datatype
        False otherwise
    """
    return df[col_name].dtype == df2[col_name].dtype


def run_live_or_frozen(
    df: pd.DataFrame,
    target: str or list[str],
    error_marker: str,
    state: str = "live",
    error_values: List[str] = ["E", "W"],
) -> pd.DataFrame:
    """
    For frozen, therefore target values are converted to null, hence responses
    in error are treated as non-response.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    target : str or list[str]
        Column(s) to treat as non-response.
    error_marker : str
        Column name with error values.
    state : str, optional
        Function config parameter. The default is "live". "live" state won't do
        anyting, "frozen" will convert to null the error_values within error_marker
    error_values : list[str], optional
        Values to ignore. The default is ['E', 'W'].

    Returns
    -------
    Original dataframe.

    """

    if state not in ["frozen", "live"]:
        raise ValueError(
            """{} is not an accepted state status, use either frozen or live """.format(
                state
            )
        )

    if state == "frozen":

        df.loc[df[error_marker].isin(error_values), target] = np.nan

    return df


def convert_annual_thousands(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Convert values from annual £000s to monthly £.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    col : str
        Col name of df.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe.

    """

    df[col] = df[col] * 1000 / 12

    return df


def create_imputation_class(
    df: pd.DataFrame, cell_no_col: str, new_col: str
) -> pd.DataFrame:
    """
    Replaces the first character '7' with '5' and removes the last character in
    all values in a column.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    cell_no_col : str
        Column name of df.
    new_col : str
        Column name to save the results.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with new_col.
    """
    df[new_col] = (
        df[cell_no_col]
        .astype(str)
        .map(lambda x: str(5) + x[1:-1] if x[0] == str(7) else x[:-1])
        .astype(int)
    )

    return df
