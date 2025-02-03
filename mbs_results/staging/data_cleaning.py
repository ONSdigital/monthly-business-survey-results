import warnings
from typing import List

import pandas as pd

from mbs_results.utilities.utils import convert_column_to_datetime
from mbs_results.utilities.validation_checks import (
    validate_indices,
    validate_manual_constructions,
)


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
    responses = responses[list(responses_keep_cols)].set_index([reference, period])
    contributors = contributors[list(contributors_keep_cols)].set_index(
        [reference, period]
    )

    validate_indices(responses, contributors)
    return responses.merge(contributors, on=[reference, period])


def enforce_datatypes(
    df: pd.DataFrame,
    keep_columns: list,
    master_column_type_dict: dict,
    temporarily_remove_cols: list,
    **config,
):
    """
    function to change datatypes of columns based on config file

    Parameters
    ----------
    df : pd.DataFrame
        dataframe with combined responses and contributors columns
        index is expected to be 'period' and 'reference'
    keep_columns : dict
        dictionary containing columns to keep and datatypes
    temporarily_remove_cols : list
        list containing column names to drop.
        Implemented to remove columns while not removing datatypes

    Returns
    -------
    pd.DataFrame
        dataframe with correctly formatted column datatypes.
    """
    subset_dict = dict(
        (k, master_column_type_dict[k])
        for k in keep_columns
        if k in master_column_type_dict
    )

    # Resetting to easily change datatypes

    df_convert = df.copy()
    df_convert = df_convert.reset_index()
    if "index" in df_convert.columns.tolist():
        df_convert.drop("index", axis=1, inplace=True)

    # Joining Dicts will overwrite first dict if values are different.
    # check for this is carried out in 'validate_config_repeated_datatypes'

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
        keep_columns.pop(key1, None)

    for key in subset_dict:
        type_from_dict = subset_dict[key]
        if type_from_dict in ["str", "float", "bool", "category"]:
            df_convert[key] = df_convert[key].astype(type_from_dict)
        elif type_from_dict == "int":
            df_convert[key] = df_convert[key].astype("int64")
        elif type_from_dict == "date":
            df_convert[key] = convert_column_to_datetime(df_convert[key])
    # Re-set the index back to reference and period
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
    question_no: str,
    target: str,
    **config,
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
    question_no: str
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

        df = df.merge(
            manual_constructions_filter,
            on=[reference, period],
            how="left",
            suffixes=("", "_man_from_file"),
        ).reset_index()

        duplicate_mc_test = (
            df[f"{target}_man"].mul(df[f"{target}_man_from_file"]).notna()
        )

        if duplicate_mc_test.any():
            warnings.warn(
                f"""There is a manual construction in the backdata and
          mc file for the same reference period
          {df[duplicate_mc_test]}"""
            )

        df[f"{target}_man"] = df[f"{target}_man"].combine_first(
            df[f"{target}_man_from_file"]
        )

        df.drop(f"{target}_man_from_file", axis=1, inplace=True)

        return df


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
    status: str,
    state: str = "live",
    error_values: List[str] = ["Check needed"],
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
    status : str
        Column containing error status.
    state : str, optional
        Function config parameter. The default is "live". "live" state won't do
        anyting, "frozen" will convert to null the error_values within status
    error_values : list[str], optional
        Values to ignore. The default is ['Check needed'].
        Mapping:
            'Check needed' : '201', ("E" or "W" for CSW)
            'Clear' : '210',
            'Clear - overridden' : '211'

    Returns
    -------
    Original dataframe.

    """

    df = df.copy()

    if state not in ["frozen", "live"]:
        raise ValueError(
            """{} is not an accepted state status, use either frozen or live """.format(
                state
            )
        )

    if state == "frozen":
        df[f"live_{target}"] = df[target].copy()
        df[target] = df.apply(
            lambda x: x[target] if x[status] not in (error_values) else None, axis=1
        )

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
    df[new_col] = df[cell_no_col].astype(str).map(lambda x: x[:-1]).astype(int)

    return df


def convert_cell_number(df: pd.DataFrame, cell_number: str):
    """
    Convert NI and GB cell numbers to UK by changing the first digit to 5 if this is 7.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with cell_number column to convert
    cell_number : str
        Column name for cell_number in df

    Returns
    -------
    pd.DataFrame
        Dataframe with converted cell_number column (and original cell_number
        in separate column)

    """
    df["ni_gb_cell_number"] = df[cell_number]
    df[cell_number] = (
        df[cell_number]
        .astype(str)
        .map(lambda x: str(5) + x[1:] if x[0] == str(7) else x)
        .astype(int)
    )

    return df


def is_census(calibration_group: pd.Series, extra_bands: List) -> pd.Series:
    """
    Returns a bool series indicating if calibration group is considered cencus
    or not.

    Calibration groups in extra_bands list are considered cencus groups.

    Calibration groups ending with 4 or 5 are considered cencus groups.


    Parameters
    ----------
    calibration_group : pd.Series
        Series with calibration groups
    extra_bands: List
        Extra calibration groups which are cencus

    Returns
    -------
    pd.Series
        A bool series, TRUE if calibration group is cencus
    """

    rule_band_4_5 = calibration_group.astype(str).map(lambda x: x.endswith(("4", "5")))

    rule_extra_bands = calibration_group.isin(extra_bands)

    return rule_band_4_5 | rule_extra_bands


def filter_out_questions(
    df: pd.DataFrame, column: str, questions_to_filter: List[int], save_full_path: str
) -> pd.DataFrame:
    """
    Removes questions defined in `questions_to_filter` from df. The removed
    questions are saved in `save_full_path`.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    column : str
        Column name to search for questions.
    questions_to_filter : List(int)
        List of questions to removes.
    save_full_path : str
        Full path to save removeed values, e.g. `folder1/folder2/mydata.csv`.

    Returns
    -------
    keep_questions_df : pd.DataFrame
        Original dataframe without questions_to_filter questions.

    """
    if not save_full_path.endswith(".csv"):
        raise ValueError(
            "Function argument {} is not a csv file.".format(save_full_path)
        )

    filter_out_questions_df = df[df[column].isin(questions_to_filter)]

    keep_questions_df = df[~df[column].isin(questions_to_filter)]

    filter_out_questions_df.to_csv(save_full_path, index=False)

    keep_questions_df.reset_index(drop=True, inplace=True)

    return keep_questions_df
