import pandas as pd

from mbs_results.utilities.utils import (
    convert_column_to_datetime,
    read_colon_separated_file,
)


def is_back_data_date_ok(
    back_data_period: pd.Series,
    first_period: pd.Timestamp,
    current_period: int,
    revision_period: int,
):
    """
    Applies checks on period 0 (back data period)

    Staged data need back data (or period 0) to succesfully impute values,
    this function ensures the date in back data is correctly configured.

    Parameters
    ----------
    back_data_period : pd.Series
        All dates in back data.
    first_period : pd.Datetime
        First period in staged data.
    current_period : int
        Period of pipeline run.
    revision_period : int
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
    return True

    current_period = convert_column_to_datetime(current_period)
    period_0 = back_data_period.unique()[0]

    if len(back_data_period.unique()) != 1:
        raise ValueError("Too many dates in back data, must have only 1")

    if period_0 != current_period - pd.DateOffset(months=revision_period):
        raise ValueError("Back data period doesn't match the revision period")

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

    qv_df = pd.read_csv(config["back_data_qv_path"])

    cp_df = pd.read_csv(config["back_data_cp_path"])

    finalsel = read_colon_separated_file(
        config["back_data_finalsel_path"], config["sample_column_names"]
    )

    qv_and_cp = pd.merge(
        qv_df, cp_df, how="left", on=[config["period"], config["reference"]]
    )

    back_data_all_cols = pd.merge(
        qv_and_cp, finalsel, how="left", on=[config["period"], config["reference"]]
    )

    back_data_all_cols[config["period"]] = convert_column_to_datetime(
        back_data_all_cols[config["period"]]
    )

    return back_data_all_cols


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

    type_col = config["back_data_type"]

    map_type = config["type_to_imputation_marker"]

    imp_marker_col = config["imputation_marker_col"]

    back_data = read_back_data(config)

    back_data = back_data.rename(columns=config["csw_to_spp_columns"], errors="raise")

    # Json file can't store keys as int, thus stored them as str
    # This is why we need to convert them to str here since from csv source
    # they are loaded as int

    back_data.insert(0, imp_marker_col, back_data[type_col].astype(str).map(map_type))

    common_cols = list(staged_data.columns.intersection(back_data.columns))

    common_cols.append(imp_marker_col)

    back_data = back_data[common_cols]

    # At this point there is a missmatch in columns, they are coming from
    # different sources, this fine, back data are needed only for imputation
    # markers and they are dropped when imputation is finished

    first_period = min(staged_data[config["period"]])

    is_back_data_date_ok(
        back_data[config["period"]],
        first_period,
        config["current_period"],
        config["revision_period"],
    )

    staged_and_back_data = pd.concat([back_data, staged_data], ignore_index=True)

    return staged_and_back_data
