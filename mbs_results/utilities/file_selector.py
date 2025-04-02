import os
from typing import List

import boto3
import pandas as pd
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import list_files

from mbs_results import logger


def generate_expected_periods(current_period: int, revision_window: int) -> List[str]:
    """
    Generate a list of expected YYYYMM periods starting from current_period for
    the given revision_window.

    Parameters
    -----------
    current_period : int
        Starting period in the format YYYYMM
    revision_window : int
        Number of months to include in the sequence of expected YYYYMM.

    Returns
    -------
    List[str]
        List of expected YYYYMM formatted strings.
    """
    logger.info(
        "Generating expected periods for review window starting from "
        f"{current_period} for {revision_window} months"
    )

    current_period = pd.to_datetime(current_period, format="%Y%m")
    end_period = current_period - pd.DateOffset(months=revision_window - 1)
    expected_periods = (
        pd.date_range(end=end_period, start=current_period, freq="-1MS")
        .strftime("%Y%m")
        .tolist()
    )

    logger.info(f"Generated expected periods: {expected_periods}")

    return expected_periods


def validate_files(
    file_path: str,
    file_prefix: str,
    expected_periods: List[str],
    config: dict,
) -> List[str]:
    """
    Validate the existence of files for the given periods and return the list
    of valid files.

    Parameters
    ----------
    file_path : str
        "c:/path/to/your/files/" or "s3://bucket-name/path/to/your/files"
    file_prefix : str
        Prefix of the file names (e.g. "finalsel" or "qv").
    expected_periods : List[str]
        List of expected periods in YYYYMM format.

    Returns
    -------
    List[str]
        List of valid file paths.

    Raises
    ------
    FileNotFoundError
        If any file is missing for the expected periods.
    """
    if config["platform"] == "s3":
        client = boto3.client("s3")
        raz_client.configure_ranger_raz(
            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
        )
        files_in_storage_system = list_files(
            client=client,
            bucket_name=config["bucket_name"],
            prefix=file_path + file_prefix,
        )
        # LIST FILES IN S3

    elif config["platform"] == "network":
        # list files in windows dir
        file_path = os.path.normpath(file_path)
        files_in_storage_system = [
            os.path.join(file_path, f)
            for f in os.listdir(file_path)
            if f.startswith(file_prefix)
        ]
    else:
        raise Exception("platform must either be 's3' or 'network'")

    valid_files = [
        f
        for f in files_in_storage_system
        if f.split(".")[0].endswith(tuple(expected_periods))
    ]

    if len(valid_files) != len(expected_periods):
        found_periods = [f.split("_")[-1] for f in files_in_storage_system]
        missing_periods = list(set(expected_periods) - set(found_periods))
        missing_periods = sorted(missing_periods)
        error_string = (
            rf"Missing {file_prefix} file for periods: {', '.join(missing_periods)}"
        )
        logger.error(error_string)
        # period(s): [2021, 2022, 2023]
        raise FileNotFoundError(error_string)

    return valid_files


def find_files(
    file_path: str,
    file_prefix: str,
    current_period: int,
    revision_window: int,
    config: dict,
) -> List[str]:
    """
    Find and validate universe or finalsel files based on the given configuration.

    Parameters
    ----------
    config : dict
        Dictionary containing the following keys of interest:
        - population_path : str, optional
            File prefix pattern for universe files (e.g., "universe009").
            This contains population frame data.
        - sample_path : str, optional
            File prefix pattern for finalsel files (e.g., "finalsel009").
            This contains sample data.
        - current_period : int
            Starting period in YYYYMM format (e.g., 202401).
        - revision_window : int
            Number of months to include in the sequence of expected YYYYMM.
    file_type : str
        One of ["universe", "finalsel"]. Determines which file type to scan.
    population_path : str, optional
        Default path for universe files if not provided in the config.
    sample_path : str, optional
        Default path for finalsel files if not provided in the config.

    Returns
    -------
    List[str]
        List of file paths matching the specified file type.

    Raises
    ------
    FileNotFoundError
        If any of the files are missing for the expected periods.
    ValueError
        If the file_type is not one of "universe" or "finalsel".
    """
    logger.info(f"Starting file selection for file type: {file_prefix}")

    expected_periods = generate_expected_periods(current_period, revision_window)
    # try:

    valid_files = validate_files(
        file_path=file_path,
        file_prefix=file_prefix,
        expected_periods=expected_periods,
        config=config,
    )

    logger.info("File selection completed successfully")
    return valid_files

    # except FileNotFoundError as e:
    #     logger.exception(f"An error occurred during file selection: {e}")
    #     raise e
