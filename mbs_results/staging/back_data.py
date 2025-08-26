import warnings

import pandas as pd

from mbs_results import logger
from mbs_results.staging.data_cleaning import (
    create_form_type_spp_column,
    enforce_datatypes,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.utilities.inputs import read_colon_separated_file, read_csv_wrapper
from mbs_results.utilities.utils import convert_column_to_datetime


def is_back_data_date_ok(
    back_data_period: pd.Series,
    first_period: pd.Timestamp,
    current_period: int,
    revision_window: int,
):
    """
    Applies checks on period 0 (back data period)

    Staged data need back data (or period 0) to succesfully impute values,
    this function ensures the date in back data is correctly configured.

    Parameters
    ----------
    back_data_period : pd.Series
        All dates in back data.
    first_period : pd.Timestamp
        First period in staged data.
    current_period : int
        Period of pipeline run.
    revision_window : int
        Revision length of staged data.

    Raises
    ------
    ValueError
        Raises values errors:
            1. Multiple dates in back data period
            2. First staged period is not next month of period 0
            3. Period 0 does not match current period and revision period

    Returns
    -------
    None.

    """

    current_period = convert_column_to_datetime(current_period)
    period_0 = back_data_period.unique()[0]

    if len(back_data_period.unique()) != 1:
        raise ValueError("Too many dates in back data, must have only 1")

    if period_0 != current_period - pd.DateOffset(months=revision_window):
        raise ValueError("Back data period doesn't match the revision window")

    if first_period != period_0 + pd.DateOffset(months=1):
        raise ValueError(
            """Staged data first period is not next month of
                         back data period"""
        )

    return True


def read_back_data(config: dict) -> pd.DataFrame:
    """Loads all required files for back data as a dataframe.

    Parameters
    ----------
    config : dict
        Pipeline confiration, needs the following defined there
        1. qv path
        2. cp path
        3. sample (final selection) path
        4. period column name
        5. reference column name

    Returns
    -------
    back_data_all_cols : pd.DataFrame
        Back data with all column as in source, period is converted to datetime.
    """

    if config["back_data_format"] == "json":
        cp_df, qv_df = get_dfs_from_spp(
            config["back_data_qv_cp_json_path"], config["platform"], config["bucket"]
        )
        cp_df = cp_df.drop(
            columns=[
                "cell_no",
                "classification",
                "createdby",
                "createddate",
                "lastupdatedby",
                "lastupdateddate",
            ],
            errors="ignore",
        )
        qv_df = qv_df.drop(
            columns=[
                "cell_no",
                "classification",
                "createdby",
                "createddate",
                "lastupdatedby",
                "lastupdateddate",
                "survey",
                "formtype",
            ],
            errors="ignore",
        )

    elif config["back_data_format"] == "csv":
        qv_df = read_csv_wrapper(
            config["back_data_qv_path"], config["platform"], config["bucket"]
        ).drop(columns=["cell_no", "classification"], errors="ignore")

        cp_df = read_csv_wrapper(
            config["back_data_cp_path"], config["platform"], config["bucket"]
        ).drop(columns=["cell_no", "classification"], errors="ignore")

    else:
        raise Exception("back_data_format must be either 'csv' or 'json'")

    qv_df[config["period"]] = convert_column_to_datetime(qv_df[config["period"]])
    cp_df[config["period"]] = convert_column_to_datetime(cp_df[config["period"]])

    # enforce all data types here

    if "yes" in qv_df[config["target"]] or "no" in qv_df[config["target"]]:
        qv_df[config["target"]] = (
            qv_df[config["target"]]
            .map({"yes": 1, "no": 0})
            .fillna(qv_df[config["target"]])
        )

    cp_df = enforce_datatypes(
        cp_df,
        keep_columns=config["contributors_keep_cols"],
        **config,
    )

    qv_df = enforce_datatypes(
        qv_df, keep_columns=config["responses_keep_cols"], **config
    )

    finalsel = read_colon_separated_file(
        filepath=config["back_data_finalsel_path"],
        column_names=config["sample_column_names"],
        keep_columns=config["finalsel_keep_cols"],
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )
    # keep columns is applied in data reading from source, enforcing dtypes
    # in all columns of finalsel
    finalsel = enforce_datatypes(finalsel, keep_columns=list(finalsel), **config)

    join_type = "left"

    cp_finalsel = pd.merge(
        cp_df, finalsel, how=join_type, on=[config["period"], config["reference"]]
    )

    qv_and_cp = pd.merge(
        qv_df, cp_finalsel, how="right", on=[config["period"], config["reference"]]
    )

    return qv_and_cp


def append_back_data(staged_data: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Concats back data with staged data, also converts type column to equivalent
    imputation marker as defined in the config.

    Parameters
    ----------
    staged_data : pd.DataFrame
        Dataframe to be imputed.
    config : dict
        Pipeline configuration.

    Returns
    -------
    staged_and_back_data : pd.DataFrame
        Staged data with back data.
    """

    back_data = read_and_process_back_data(config)

    # Remove derived, derived values not needed
    # Having derivedd values in back data will throw an error in imputation flags
    if "derived" in back_data[config["imputation_marker_col"]].unique():
        back_data = back_data[back_data[config["imputation_marker_col"]] != "derived"]
        warnings.warn("Removing derived values from back")

    common_cols = list(staged_data.columns.intersection(back_data.columns))

    common_cols.append(config["imputation_marker_col"])

    back_data = back_data[common_cols]

    # At this point there is a missmatch in columns, they are coming from
    # different sources, this fine, back data are needed only for imputation
    # markers and they are dropped when imputation is finished

    first_period = min(staged_data[config["period"]])

    is_back_data_date_ok(
        back_data[config["period"]],
        first_period,
        config["current_period"],
        config["revision_window"],
    )

    staged_and_back_data = pd.concat([back_data, staged_data], ignore_index=True)

    return staged_and_back_data


def read_and_process_back_data(config: dict) -> pd.DataFrame:
    """
    Read in back data, change column names inline with SPP column names
    add imputation marker column based on "imputation_marker_col" from config

    Parameters
    ----------
    config : dict
        main pipeline config

    Returns
    -------
    back_data: pd.DataFrame
        processed back data dataframe
    """
    type_col = config["back_data_type"]

    map_type = config["type_to_imputation_marker"]

    back_data = read_back_data(config)

    if config["back_data_format"] == "csv":
        # renaming columns to spp standard names
        # this is only needed for csv back data
        back_data = back_data.rename(
            columns=config["csw_to_spp_columns"], errors="raise"
        )

    # Json file can't store keys as int, thus stored them as str
    # This is why we need to convert them to str here since from csv source
    # they are loaded as int
    # Filled as -999 because int cannot store nulls, and -999 isnt a used type
    if config["back_data_format"] == "csv":
        back_data.insert(
            0,
            config["imputation_marker_col"],
            back_data[type_col].fillna(-999).astype(int).astype(str).map(map_type),
        )
    elif config["back_data_format"] == "json":
        back_data.insert(
            0,
            config["imputation_marker_col"],
            back_data["imputationmarker"],
        )

    # TO-DO: Refactor this so construction pipeline doesn't get an unnecessary warning
    if "idbr_to_spp" in config:
        create_form_type_spp_column(back_data, config)
    else:
        logger.info(
            """idbr_to_spp column not specified in config, so skipping
            create_form_type_spp_column. You can safely ignore this if
            this was intentional, i.e. running the construction pipeline."""
        )

    back_data["cellnumber"] = back_data["cell_no"]

    return back_data
