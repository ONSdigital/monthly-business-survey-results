import inspect
import os
import re
from typing import List

import boto3
import pandas as pd
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import load_csv

from mbs_results import configure_logger_with_run_id, logger
from mbs_results.utilities.merge_two_config_files import merge_two_config_files
from mbs_results.utilities.utils import get_datetime_now_as_int


def load_config(config_user_path, config_user_dict=None):
    """Load the dev and user configs and merges into one dictionary"""
    # Get the directory of the script that called this function
    # This is necessary to find the path of the config files relative to this script
    caller_frame = inspect.stack()[1]
    caller_file = caller_frame.filename
    caller_dir = os.path.dirname(os.path.abspath(caller_file))
    logger.info(f"load_config caller directory: {caller_dir}")

    # Get config paths relative to the caller directory and check if the files exist
    if not os.path.exists(config_user_path):
        warning_message = f"config_user.json not found at {config_user_path}"
        logger.warning(warning_message)

    config_dev_path = os.path.join(caller_dir, "configs", "config_dev.json")
    if not os.path.exists(config_dev_path):
        error_message = f"config_dev.json not found at {config_dev_path}"
        logger.error(error_message)
        raise FileNotFoundError(error_message)

    config = merge_two_config_files(config_user_path, config_dev_path, config_user_dict)
    logger.info(
        f"config dictionary created from merging {config_user_path} "
        f"and {config_dev_path}"
    )
    # set run id as YYYYMMDDHHMM
    config["run_id"] = get_datetime_now_as_int()
    configure_logger_with_run_id(config)

    if config_user_dict is not None:
        config.update(config_user_dict)
        logger.info(
            "config dictionary updated with config user dictionary from the testing"
        )

    logger.info("Appending config files with selected sic column name")
    config["finalsel_keep_cols"].append(config["sic"])
    config["population_keep_columns"].append(config["sic"])

    return config


def validate_colon_file_columns(
    filepath: str,
    column_names: List[str],
    import_platform: str = "network",
    bucket_name: str = None,
) -> pd.DataFrame:
    """Check if column_names match the columns in filepath.

    Parameters
    ----------
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket or
        in the network.
    column_names : List[str]
        list of column names in data file.
    import_platform : str
        Platform to import from. Must be either 's3' or 'network'
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.
    kwargs
        Additional keyword arguments to pass to the `pd.read_csv` method.

    Raises
    ------
    Exception
       If length of columns is not alligned with the number of columns when
       the dataframe is loaded.
    """

    df = read_csv_wrapper(
        filepath,
        import_platform,
        bucket_name,
        sep=":",
        names=column_names,
        nrows=1,
    )

    if len(df.columns) is not len(column_names):
        raise Exception(
            "Length of `column_names` is: ",
            len(column_names),
            "does not match the columns in file: ",
            filepath,
        )


def read_csv_wrapper(
    filepath: str,
    import_platform: str = "network",
    bucket_name: str = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Load a CSV file from an S3 bucket or from a network path into a Pandas
    DataFrame.

    Parameters
    ----------
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket or
        in the network.
    import_platform : str
        Platform to import from. Must be either 's3' or 'network'
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.
    kwargs
        Additional keyword arguments to pass to the `pd.read_csv` method.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing the data from the CSV file.

    Raises
    ------
    Exception
        If import_platform is not either be 's3' or 'network'.
    """
    if import_platform == "s3":
        client = boto3.client("s3")
        raz_client.configure_ranger_raz(
            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
        )
        df = load_csv(
            client=client, bucket_name=bucket_name, filepath=filepath, **kwargs
        )

        return df

    if import_platform == "network":
        df = pd.read_csv(filepath, **kwargs)
        return df

    raise Exception("platform must either be 's3' or 'network'")


def read_colon_separated_file(
    filepath: str,
    column_names: List[str],
    keep_columns: List[str] = None,
    period="period",
    import_platform: str = "network",
    bucket_name: str = None,
) -> pd.DataFrame:
    """
    Load a CSV file from an S3 bucket or from a network path into a Pandas
    DataFrame.

    Parameters
    ----------
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket or
        in the network.
    column_names : List[str]
        list of column names in data file
    keep_columns : List[str], optional
        list of column names to keep, must be a subset of column_names.
    import_platform : str
        Platform to import from. Must be either 's3' or 'network'
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing the data from a colon seperated file.
    Raises
    ------
    Exception
       If `keep_columns` is provided then raises an exception when it's not a
       subset of `column_names`
    """
    usecols = None  # pd.read_csv default load all columns

    if keep_columns:

        if not set(keep_columns).issubset(set(column_names)):
            raise Exception(keep_columns, " must be a subset of ", column_names)

        # position of columns to keep
        usecols = [column_names.index(x) for x in keep_columns]

        # pd.reader ingores order, usecols=[2,0,1] is same as [0,1,2]
        # ordered column names (must align with usecols)
        column_names = [x for _, x in sorted(zip(usecols, keep_columns))]

    validate_colon_file_columns(filepath, column_names, import_platform, bucket_name)

    df = read_csv_wrapper(
        filepath,
        import_platform,
        bucket_name,
        sep=":",
        names=column_names,
        usecols=usecols,
    )

    # Esure the filepath is a string
    if not isinstance(filepath, str):
        if isinstance(filepath, os.PathLike):
            filepath = str(filepath)
        else:
            error_msg = (
                "The filepath must be a string or os.PathLike object. "
                f"Got {type(filepath)} instead."
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

    date_string = re.findall(r"_(\d{6})", filepath)

    # Get pattern from end, to avoid issues when path has dates
    # e.g. path_190812/file_202301 should return 202301

    if date_string:
        df[period] = int(date_string[-1])
    else:
        error_msg = (
            "The filepath does not contain a date string in the format "
            f"'_YYYYMM'. Please check the {filepath}."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    return df
