import fnmatch
from os import listdir
from os.path import isfile, join

import pandas as pd
from mbs_results.utilities.utils import convert_column_to_datetime


def get_patern_df(filepath: str, pattern: str) -> pd.DataFrame:
    """Loads as pd dataframe all csv files with pattern.

    Parameters
    ----------
    filepath : str
        Filepath to folder containg desired files.
    pattern : str
        Regex pattern to filter files in the folder based on name.

    Returns
    -------
    pd.DataFrame
        Dataframe containg data from all selected files.
    """

    filenames = [
        filename for filename in listdir(filepath) if isfile(join(filepath, filename))
    ]
    filenames = fnmatch.filter(filenames, pattern)
    df_list = [pd.read_csv(filepath + "/" + filename) for filename in filenames]
    df = pd.concat(df_list, ignore_index=True)

    return df


def get_qv_and_cp_data(
    cp_path: str,
    qv_path: str,
) -> pd.DataFrame:
    """Reads and joins qv and cp data.

    Parameters
    ----------
    cp_path : str
        Filepath to folder containing cp data.
    qv_path : str
        Filepath to folder containing qv data.

    Returns
    -------
    pd.DataFrame
        Dataframe containing combined qv and cp data.
    """

    qv_df = get_patern_df(qv_path, "qv*.csv")
    cp_df = get_patern_df(cp_path, "cp*.csv")

    qv_and_cp = pd.merge(qv_df, cp_df, how="left", on=["period", "reference"])

    return qv_and_cp


def csw_to_spp(
    cp_path: str,
    qv_path: str,
    output_path: str,
    column_map: dict,
    period: str,
    period_range: int,
) -> None:
    """Combines cp and qv files, filters and renames columns based on a mapping, and
    then saves the output as a json file.

    Parameters
    ----------
    cp_path : str
        Filepath to folder containing cp data.
    qv_path : str
        Filepath to folder containing qv data.
    output_path : str
        Filepath to save json file.
    column_map : dict
        Dictionary containing desired columns from qv and cp data as keys and their
        desired names as values.
    period : str
        Date to filter output on (YYYY-MM-DD).
    period_range : str
        Number of months from the period and previous to include in the output.
    """
    qv_and_cp = get_qv_and_cp_data(cp_path, qv_path)

    qv_and_cp["period"] = convert_column_to_datetime(qv_and_cp["period"])

    period = pd.Timestamp(period)

    qv_and_cp = qv_and_cp[
        (qv_and_cp["period"] > period - pd.DateOffset(months=period_range))
        & (qv_and_cp["period"] <= period)
    ]

    qv_and_cp["period"] = qv_and_cp["period"].dt.strftime("%Y%m")

    qv_and_cp = qv_and_cp[column_map.keys()].rename(columns=column_map)

    qv_and_cp.to_json(f"{output_path}_{period.strftime('%Y%m')}_{period_range}.json")
