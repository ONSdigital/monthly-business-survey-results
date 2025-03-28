import glob

import pandas as pd

from mbs_results.estimation.calculate_estimation_weights import (
    calculate_calibration_factor,
    calculate_design_weight,
)
from mbs_results.estimation.create_population_counts import (
    create_population_count_output,
)
from mbs_results.estimation.pre_processing_estimation import get_estimation_data
from mbs_results.staging.data_cleaning import is_census
from mbs_results.utilities.inputs import read_csv_wrapper
# from mbs_results.estimation.validate_estimation import validate_estimation


def apply_estimation(
    population_path,
    sample_path,
    calibration_group,
    census_extra_calibration_group,
    period,
    **config
):
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
    census_extra_calibration_group: list
        calibration groups which are census but not band 4 or 5
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

    calibration_group_map = read_csv_wrapper(config["calibration_group_map_path"], config["platform"],
        config["bucket"])

    for population_file, sample_file in zip(population_files, sample_files):
        estimation_data = get_estimation_data(
            population_file,
            sample_file,
            period,
            calibration_group_map=calibration_group_map,
            **config
        )

        census_df = estimation_data[
            is_census(
                estimation_data[calibration_group], census_extra_calibration_group
            )
        ]

        census_df["design_weight"] = 1
        census_df["calibration_factor"] = 1
        census_df["is_sampled"] = True
        census_df["is_census"] = True
        # is_census: bool, to distinguish fully sampled (i.e. census) strata from
        # non-census strata. Used in outlier detection so census strata are
        # not winsorised.
        # is_sampled: bool. This is used to distinguish sampled refs from non-sampled
        # refs in population

        non_census_df = estimation_data[
            ~(
                is_census(
                    estimation_data[calibration_group], census_extra_calibration_group
                )
            )
        ]

        non_census_df = calculate_design_weight(non_census_df, period, **config)
        non_census_df = calculate_calibration_factor(non_census_df, period, **config)
        non_census_df["is_census"] = False

        all_together = pd.concat([non_census_df, census_df], ignore_index=True)

        estimation_df_list.append(all_together)

    estimation_df = pd.concat(estimation_df_list, ignore_index=True)

    create_population_count_output(estimation_df, period, save_output=True, **config)

    # validate_estimation(estimation_df, **config)

    return estimation_df
