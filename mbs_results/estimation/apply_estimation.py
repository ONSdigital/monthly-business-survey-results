import glob

import pandas as pd

from mbs_results.estimation.calculate_estimation_weights import (
    calculate_calibration_factor,
    calculate_design_weight,
)
from mbs_results.estimation.pre_processing_estimation import get_estimation_data
from mbs_results.staging.data_cleaning import is_census

# from mbs_results.estimation.validate_estimation import validate_estimation


def apply_estimation(population_path, sample_path, calibration_group, period, **config):
    """
    Read population frame and sample, merge key variables onto df then derive
    and validate estimation weights.

    Parameters
    ----------
    population_path : str
        filepath for population frame data
    sample_path : str
        filepath for sample data
    calibration_group: str
        column name of dimension contaning calibration group values
    period : str
        name of column containing period

    Returns
    -------
    population frame with calibration group, sampled flag, design weight and
    calibration factor

    Raises
    ------
    `ValueError`

    """
    population_files = glob.glob(population_path)
    sample_files = glob.glob(sample_path)

    estimation_df_list = []

    for population_file, sample_file in zip(population_files, sample_files):
        estimation_data = get_estimation_data(
            population_file, sample_file, period, **config
        )

        census_df = estimation_data[is_census(estimation_data[calibration_group])]

        census_df["design_weight"] = 1
        census_df["calibration_factor"] = 1
        census_df["sampled"] = 0

        non_census_df = estimation_data[
            ~(is_census(estimation_data[calibration_group]))
        ]

        non_census_df = calculate_design_weight(non_census_df, period, **config)
        non_census_df = calculate_calibration_factor(non_census_df, period, **config)

        all_together = pd.concat([non_census_df, census_df], ignore_index=True)

        estimation_df_list.append(all_together)

    estimation_df = pd.concat(estimation_df_list, ignore_index=True)

    # validate_estimation(estimation_df, **config)

    return estimation_df
