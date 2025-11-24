import logging
import os
from typing import List

import boto3
import pandas as pd
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import list_files

logger = logging.getLogger(__name__)


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
        "c:/path/to/your/files/" or "/path/to/your/files/"
    file_prefix : str
        Prefix of the file names (e.g. "finalsel" or "qv").
    expected_periods : List[str]
        List of expected periods in YYYYMM format.
    config : dict
        main config file for pipeline

    Returns
    -------
    List[str]
        List of valid file paths.

    Raises
    ------
    FileNotFoundError
        If any file is missing for the expected periods.

    Example
    -------
    >>> file_path = "c:/data/"
    >>> file_prefix = "finalsel"
    >>> expected_periods = ["202301", "202302", "202303"]
    >>> config = {"platform": "network"}
    >>> validate_files(file_path, file_prefix, expected_periods, config)
    ['c:/data/finalsel_202301', 'c:/data/finalsel_202302', 'c:/data/finalsel_202303']
    """
    if config["platform"] == "s3":
        # list files in windows s3 bucket
        client = boto3.client("s3")
        raz_client.configure_ranger_raz(
            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
        )
        files_in_storage_system = list_files(
            client=client,
            bucket_name=config["bucket"],
            prefix=file_path + file_prefix,
        )

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

    if len(valid_files) < len(expected_periods):
        found_periods = [f.split("_")[-1] for f in files_in_storage_system]
        missing_periods = list(set(expected_periods) - set(found_periods))
        missing_periods = sorted(missing_periods)
        error_string = (
            rf"Missing {file_prefix} file for periods: {', '.join(missing_periods)}"
        )
        logger.error(error_string)
        raise FileNotFoundError(error_string)
    elif len(valid_files) > len(expected_periods):
        # Check for duplicate files with the same period
        period_counts = {}
        for f in files_in_storage_system:
            period = f.split("_")[-1].split(".")[0]
            period_counts[period] = period_counts.get(period, 0) + 1

        duplicate_periods = [
            period for period, count in period_counts.items() if count > 1
        ]
        if duplicate_periods:
            error_string = (
                rf"Duplicate {file_prefix} files found for periods: "
                + rf"{', '.join(duplicate_periods)}"
            )
            logger.error(error_string)
            raise FileExistsError(error_string)

    return valid_files


def find_files(
    file_path: str,
    file_prefix: str,
    current_period: int,
    revision_window: int,
    config: dict,
) -> List[str]:
    """
    Find and validate files with the 'prefix_YYYYMM' format files based on the
    given configuration.

    Parameters
    ----------

    file_path : str
        Default path for data files to find.
    file_prefix
        Prefix of filename to find (e.g. "universe", "finalsel", "qv")
    current_period: int
        current period in YYYYMM format.
    revision_window: int
        Number of months to include in the sequence of expected YYYYMM.
        1 <= revision_window
    config : dict
        Dictionary containing the following keys of interest:
        platform - either "s3" or "network"
        bucket_name - S3 bucket name for file storage. (optional)
    Returns
    -------
    List[str]
        List of file paths matching the specified file type.

    Raises
    ------
    FileNotFoundError
        If any of the files are missing for the expected periods.
    """
    logger.info(f"Starting file selection for file type: {file_prefix}")

    expected_periods = generate_expected_periods(current_period, revision_window)

    valid_files = validate_files(
        file_path=file_path,
        file_prefix=file_prefix,
        expected_periods=expected_periods,
        config=config,
    )

    logger.info("File selection completed successfully")
    return sorted(valid_files)
