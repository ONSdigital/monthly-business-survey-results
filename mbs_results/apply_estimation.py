import glob

import pandas as pd

from mbs_results.calculate_estimation_weights import (
    calculate_calibration_factor,
    calculate_design_weight,
)
from mbs_results.pre_processing_estimation import get_estimation_data
from mbs_results.validate_estimation import validate_estimation


def apply_estimation(df, reference, period, population_path, sample_path, **config):
    """
    Read population frame and sample, merge key variables onto df then derive
    and validate estimation weights.

    Parameters
    ----------
    df : pd.DataFrame
        main pipeline data
    reference : str
        name of column in df containing reference
    period : str
        name of column in df containing period
    population_path : str
        filepath for population frame data
    sample_path : str
        filepath for sample_path
    strata : str
        name of column in df containing strata
    group : str
        name of column in df containing strata (for separate estimation) or
        calibration group (for combined estimation)
    auxiliary : str
        name of column in df containing auxiliary variable

    Returns
    -------
    df with calibration group, sampled flag, design weight and calibration factor

    Raises
    ------
    `ValueError`

    """
    population_files = glob.glob(population_path)
    sample_files = glob.glob(sample_path)

    estimation_df_list = []

    for population_file, sample_file in zip(population_files, sample_files):
        estimation_data = get_estimation_data(
            reference, period, population_file, sample_file, **config
        )

        estimation_data = calculate_design_weight(estimation_data, period, **config)
        estimation_data = calculate_calibration_factor(
            estimation_data, period, **config
        )

        estimation_df_list.append(estimation_data)

    estimation_df = pd.concat(estimation_df_list, ignore_index=True)

    validate_estimation(estimation_df, **config)

    df = df.merge(estimation_df, how="left", on=[reference, period])

    return df
