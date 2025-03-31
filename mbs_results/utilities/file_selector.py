from datetime import datetime, timedelta
from pathlib import Path
from typing import List

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
    current_period = datetime.strptime(str(current_period), "%Y%m")
    expected_periods = [current_period.strftime("%Y%m")]

    for i in range(1, revision_window):
        current_period -= timedelta(days=31)
        expected_periods.append(current_period.strftime("%Y%m"))

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
    file_dir = Path(file_dir)

    for period in expected_periods:
        base_file_name = f"{file_prefix}_{period}"
        file_with_ext = file_dir / f"{base_file_name}.csv"
        file_without_ext = file_dir / base_file_name

        # Check if the files exist
        if file_without_ext.is_file():
            valid_files.append(str(file_without_ext))
        elif file_with_ext.is_file():
            valid_files.append(str(file_with_ext))
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
            File prefix pattern for universe files (e.g., "universe009_*").
            This contains population frame data.
        - sample_path : str, optional
            File prefix pattern for finalsel files (e.g., "finalsel009_*").
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
            population_path = Path(config.get("population_path", population_path))
            file_prefix = population_path.stem.split("_*")[0]
            file_dir = population_path.parent

        elif file_type == "finalsel":
            sample_path = Path(config.get("sample_path", sample_path))
            file_prefix = sample_path.stem.split("_*")[0]
            file_dir = sample_path.parent
        else:
            logger.error("Invalid file type. Expected 'universe' or 'finalsel'")
            raise ValueError("Invalid file type. Expected 'universe' or 'finalsel'")

        valid_files = validate_files(file_dir, file_prefix, expected_periods, file_type)

        logger.info("File selection completed successfully")
        return valid_files

    except FileNotFoundError as e:
        logger.exception(f"An error occurred during file selection: {e}")
        raise e
