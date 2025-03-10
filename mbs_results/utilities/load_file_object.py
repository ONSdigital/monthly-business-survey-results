import os
from typing import Dict, Union

import pandas as pd

from mbs_results import logger
from mbs_results.utilities.s3_operations_utils import load_dataframe_from_s3


def load_file(
    config: Dict[str, str],
    file_path: str,
    file_format: str,
    compression: Union[str, None] = None,
) -> pd.DataFrame:
    """
    Reads a file either from S3 or from the local file system depending on the platform specified in the config.

    This function reads a file based on the specified platform in the `config` dictionary. If the platform is 's3',
    the file is loaded from an S3 bucket. If the platform is 'network', the file is loaded from the local file system.
    The function automatically handles the compression (if provided) based on the file format.

    Parameters
    ----------
    config : dict
        A dictionary containing configuration settings. This should include:
        - 'platform' (str): Specifies the platform to read from ('s3' for S3 or 'network' for local file system).
        - 's3_bucket' (str): The S3 bucket name (used only if platform is 's3').
    file_path : str
        The path to the file to be read. If the platform is 's3', this is the S3 key.
    file_format : str
        The format of the file to be read. Supported formats are 'csv', 'json', and 'parquet'.
    compression : str, optional
        The compression format used to store the file. Default is None. Compression options are:
        - 'gzip', 'bz2', 'zip', etc. for 'csv'.
        - 'gzip', 'snappy', etc. for 'parquet'.

    Returns
    -------
    pd.DataFrame
        The DataFrame read from the file.

    Raises
    ------
    ValueError
        If an unsupported platform is provided in the config or if the file format is invalid.

    Example
    -------
    config = {'platform': 's3', 's3_bucket': 'my-bucket'}
    df = load_file(config, '/path/to/local/file.csv', 'csv')
    print(df.head())

    This will load a CSV file from the local file system into a DataFrame.

    Notes
    -----
    - The function supports reading from two platforms: 's3' (S3) or 'network'. If an invalid platform is provided,
      a `ValueError` will be raised.
    - Supported file formats are 'csv', 'json', and 'parquet'. If an unsupported file format is specified, a `ValueError`
      will be raised.
    """

    if config["platform"] == "s3":
        return load_dataframe_from_s3(
            bucket_name=config["s3_bucket"],
            s3_key=file_path,
            file_format=file_format,
            compression=compression,
        )
    elif config["platform"] == "network":
        return load_dataframe_locally(
            file_path=file_path, file_format=file_format, compression=compression
        )
    else:
        raise ValueError("Unknown platform. Only 's3' (S3) or 'network' are supported.")


def load_dataframe_locally(
    file_path: str, file_format: str, compression: Union[str, None] = None
) -> pd.DataFrame:
    """
    Reads a DataFrame from the local file system in the specified format.

    This function reads a file from the local file system and loads it into a pandas DataFrame. It supports
    reading files in 'csv', 'json', and 'parquet' formats. Compression can be specified if the file is compressed.

    Parameters
    ----------
    file_path : str
        The path to the file to be read. The file should be located on the local file system.
    file_format : str
        The format of the file to be read. Supported formats are:
        - 'csv': Reads as CSV.
        - 'json': Reads as JSON (with records-oriented format and lines=True).
        - 'parquet': Reads as Parquet.
    compression : str, optional
        The compression format used to store the file. Default is None. Supported compression options depend on the file format:
        - For 'csv': 'gzip', 'bz2', 'zip', etc.
        - For 'parquet': 'gzip', 'snappy', etc.

    Returns
    -------
    pd.DataFrame
        The DataFrame read from the file.

    Raises
    ------
    ValueError
        If an unsupported file format is specified.
    Exception
        If an error occurs while reading the file (e.g., file not found, read error).

    Example
    -------
    df = load_dataframe_locally('/path/to/file.csv', 'csv', compression='gzip')
    print(df.head())

    This will read a gzipped CSV file from the specified path into a DataFrame.

    Notes
    -----
    - The function automatically handles compression when specified, depending on the file format.
    - Supported file formats are 'csv', 'json', and 'parquet'. Any other file format will raise a `ValueError`.
    """

    try:
        if file_format == "csv":
            return pd.read_csv(file_path, compression=compression)
        elif file_format == "json":
            return pd.read_json(file_path, lines=True)
        elif file_format == "parquet":
            return pd.read_parquet(file_path)
        else:
            raise ValueError(
                "Invalid file format. Supported formats are 'csv', 'json', or 'parquet'."
            )

    except Exception as e:
        logger.error(
            f"An error occurred while reading DataFrame locally from {file_path}: {e}"
        )
        raise e
