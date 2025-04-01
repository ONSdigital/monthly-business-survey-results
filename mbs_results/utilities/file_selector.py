import os
from typing import List

import pandas as pd

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
    file_dir: str, file_prefix: str, expected_periods: List[str], file_type: str
) -> List[str]:
    """
    Validate the existence of files for the given periods and return the list
    of valid files.

    Parameters
    ----------
    file_dir : str
        Directory where the files are located.
    file_prefix : str
        Prefix of the file names.
    expected_periods : List[str]
        List of expected periods in YYYYMM format.
    file_type : str
        Type of file being validated ("universe" or "finalsel").

    Returns
    -------
    List[str]
        List of valid file paths.

    Raises
    ------
    FileNotFoundError
        If any file is missing for the expected periods.
    """
    valid_files = []
    file_dir = os.path.normpath(file_dir)

    for period in expected_periods:
        base_file_name = f"{file_prefix}_{period}"
        file_with_ext = os.path.join(file_dir, f"{base_file_name}.csv")
        file_without_ext = os.path.join(file_dir, base_file_name)

        # Check if the files exist
        if os.path.isfile(file_without_ext):
            valid_files.append(file_without_ext)
        elif os.path.isfile(file_with_ext):
            valid_files.append(file_with_ext)
        else:
            logger.error(f"Missing {file_type} file for period: {period}")
            raise FileNotFoundError(f"Missing {file_type} file for period: {period}")

    return valid_files


def find_files(
    config: dict,
    file_type: str,
    population_path: str = None,
    sample_path: str = None,
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
    logger.info(f"Starting file selection for file type: {file_type}")

    current_period = config["current_period"]
    revision_window = config["revision_window"]

    expected_periods = generate_expected_periods(current_period, revision_window)

    try:
        if file_type == "universe":
            population_path = config.get("population_path", population_path)
            population_path = os.path.normpath(population_path)
            file_prefix = os.path.basename(population_path).split("_*")[0]
            file_dir = os.path.dirname(population_path)

        elif file_type == "finalsel":
            sample_path = config.get("sample_path", sample_path)
            sample_path = os.path.normpath(sample_path)
            file_prefix = os.path.basename(sample_path).split("_*")[0]
            file_dir = os.path.dirname(sample_path)
        else:
            logger.error("Invalid file type. Expected 'universe' or 'finalsel'")
            raise ValueError("Invalid file type. Expected 'universe' or 'finalsel'")

        valid_files = validate_files(file_dir, file_prefix, expected_periods, file_type)

        logger.info("File selection completed successfully")
        return valid_files

    except FileNotFoundError as e:
        logger.exception(f"An error occurred during file selection: {e}")
        raise e
