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
from mbs_results.utilities.file_selector import find_files
from mbs_results.utilities.inputs import read_csv_wrapper

# from mbs_results.estimation.validate_estimation import validate_estimation


def apply_estimation(method: str, convert_NI_GB_cells: bool, config: dict):
    """
    Read population frame and sample, merge key variables onto df then derive
    and validate estimation weights.

    Parameters
    ----------
    method : str
        Method to be applied, accepted values are `seperate` and `combined`
        For separate, calibration factor is calculated at strata variable
        For combined ratio,calibration factor is calculated at calibration
        group, calibration group mapping must be supplied via a csv file.
    convert_NI_GB_cells: bool
        If True, will convert NI and GB cells to UK (convert_cell_number
        will be activated)
    config : dict
        main config file for pipeline

    Returns
    -------
    population frame with calibration group, sampled flag, design weight and
    calibration factor

    Raises
    ------
    `ValueError`

    """
    population_files = find_files(
        file_path=config["idbr_folder_path"],
        file_prefix=config["population_prefix"],
        current_period=config["current_period"],
        revision_window=config["revision_window"],
        config=config,
    )
    sample_files = find_files(
        file_path=config["idbr_folder_path"],
        file_prefix=config["sample_prefix"],
        current_period=config["current_period"],
        revision_window=config["revision_window"],
        config=config,
    )

    estimation_df_list = []

    if method not in ["seperated", "combined"]:
        raise ValueError(
            """{} is not an accepted state status,
              use either seperated or combined""".format(
                method
            )
        )

    if method == "combined":

        # Combined methods requires a group variable (aka calibration_group)
        # this is mapped to the main dataframe now and is used to calculate
        # calibration factor

        calibration_group_map = read_csv_wrapper(
            config["calibration_group_map_path"], config["platform"], config["bucket"]
        )

    if method == "seperated":
        # No mapping required and group is same as strata, so strata is used
        # to calculate the calibration factor

        calibration_group_map = None
        config["group"] = config["strata"]

    for population_file, sample_file in zip(population_files, sample_files):
        estimation_data = get_estimation_data(
            population_file,
            sample_file,
            calibration_group_map,
            convert_NI_GB_cells,
            config,
        )

        census_df = estimation_data[
            is_census(
                estimation_data[config["calibration_group"]],
                config["census_extra_calibration_group"],
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
                    estimation_data[config["calibration_group"]],
                    config["census_extra_calibration_group"],
                )
            )
        ]

        non_census_df = calculate_design_weight(non_census_df, **config)
        non_census_df = calculate_calibration_factor(non_census_df, **config)
        non_census_df["is_census"] = False

        all_together = pd.concat([non_census_df, census_df], ignore_index=True)

        estimation_df_list.append(all_together)

    estimation_df = pd.concat(estimation_df_list, ignore_index=True)

    create_population_count_output(estimation_df, save_output=True, **config)

    # validate_estimation(estimation_df, **config)

    return estimation_df
