import os
from typing import List, Tuple
from datetime import (
    datetime,
    timedelta
)
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
    logger.info("Generating expected periods for review window starting from "
                f"{current_period} for {revision_window} months"
                )
    current_period = datetime.strptime(str(current_period), "%Y%m")
    expected_periods = [current_period.strftime("%Y%m")]

    for i in range(1, revision_window):
        current_period += timedelta(days=31)
        expected_periods.append(current_period.strftime("%Y%m"))

    logger.info(f"Generated expected periods: {expected_periods}")

    return expected_periods


def find_files(config: dict) -> Tuple:
    """
    Function find_files finds and validates universe and finalsel files based on
    the given configuration.

    Parameters
    ----------
    config : dict
        Dictionary containing the following keys of interest, among other keys:
        - population_path : str
            File prefix pattern for universe files (e.g. "universe009_*").
            This contains population frame data
        - sample_path : str
            File prefix pattern for finalsel files (e.g. "finalsel009_*").
            This contains sample data
        - current_period : int
            Starting period in YYYYMM format (e.g. "202401")
        - revision_window : int
            Number of months to include in the sequence of expected YYYYMM.

    Returns
    -------
    Tuple[List[str], List[str]]: (universe_files, finalsel_files)
        List of universe files and list of finalsel files matching file paths.

    Raises
    ------
    `FileNotFoundError`
        If any of the universe or finalsel files are missing for the expected periods.
    `ValueError`
        If the umber of universe files or finalsel files does not match the expected
        review window.
    """

    logger.info("Starting file selection based on the given configuration")
    population_path = config["population_path"]
    sample_path = config["sample_path"]
    current_period = config["current_period"]
    revision_window = config["revision_window"]

    expected_periods = generate_expected_periods(current_period, revision_window)

    universe_prefix = population_path.split("_*")[0]
    finalsel_prefix = sample_path.split("_*")[0]

    # Get the parent directory of the population and sample files
    population_file_dir = os.path.dirname(population_path)
    sample_file_dir = os.path.dirname(sample_path)

    universe_files = []
    finalsel_files = []

    try:
        # Loop through the expected periods and check if the corresponding files exist
        for period in expected_periods:
            universe_file = os.path.join(
                population_file_dir, f"{universe_prefix}_{period}"
            )
            finalsel_file = os.path.join(
                sample_file_dir, f"{finalsel_prefix}_{period}.csv"
            )

            if not os.path.isfile(universe_file):
                logger.error(
                    f"Missing universe file {universe_file} for period: {period}"
                )
                raise FileNotFoundError(
                    f"Missing universe file for period: {period}"
                )
            universe_files.append(universe_file)

            if not os.path.isfile(finalsel_file):
                logger.error(
                    f"Missing finalsel file {finalsel_file} for period: {period}"
                )
                raise FileNotFoundError(
                    f"Missing finalsel file for period: {period}"
                )
            finalsel_files.append(finalsel_file)

        if (
            len(universe_files) == revision_window
            and len(finalsel_files) == revision_window
        ):
            logger.info(
                "Successfully found all files for both universe and finalsel files"
            )
            logger.info(f"Length of Universe files: {len(universe_files)}")
            logger.info(f"Length of Finalsel files: {len(finalsel_files)}")
            return universe_files, finalsel_files
        else:
            logger.error(
                "Number of universe files or finalsel files does not match the "
                "expected review window"
            )
            logger.info(f"Length of Universe files: {len(universe_files)}")
            logger.info(f"Length of Finalsel files: {len(finalsel_files)}")
            raise ValueError(
                "Number of universe files or finalsel files does not match "
                "the expected review window"
            )
    except FileNotFoundError as e:
        logger.exception(f"An error occured during file selection: {e}")
        raise e
