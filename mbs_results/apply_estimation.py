import glob

import pandas as pd

from mbs_results.estimation.calculate_estimation_weights import (
    calculate_calibration_factor,
    calculate_design_weight,
)
from mbs_results.estimation.pre_processing_estimation import get_estimation_data

# from mbs_results.validate_estimation import validate_estimation


def apply_estimation(population_path, sample_path, period, **config):
    """
    Read population frame and sample, merge key variables onto df then derive
    and validate estimation weights.

    Parameters
    ----------
    population_path : str
        filepath for population frame data
    sample_path : str
        filepath for sample data
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

        estimation_data = calculate_design_weight(estimation_data, period, **config)
        estimation_data = calculate_calibration_factor(
            estimation_data, period, **config
        )

        estimation_df_list.append(estimation_data)

    estimation_df = pd.concat(estimation_df_list, ignore_index=True)

    # validate_estimation(estimation_df, **config)

    return estimation_df
