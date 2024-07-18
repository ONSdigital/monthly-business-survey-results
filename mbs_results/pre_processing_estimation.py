from glob import glob

import pandas as pd

from mbs_results.utils import read_colon_separated_file


def get_estimation_data(
    population_path,
    population_column_names,
    sample_path,
    sample_column_names,
    calibration_group_map,
    period,
    reference,
    cell_number,
    **config
):

    population_files = glob("universe.*", root_dir=population_path)
    sample_files = glob("finalsel.*", root_dir=sample_path)

    population_dfs = []
    for file in population_files:

        population_df = read_colon_separated_file(file, population_column_names)

        population_dfs.append(population_df)

    population = pd.concat(population_dfs, ignore_index=True)

    sample_dfs = []
    for file in sample_files:

        sample_df = read_colon_separated_file(file, sample_column_names)

        sample_dfs.append(sample_df)

    sample = pd.concat(sample_dfs, ignore_index=True)

    estimation_data = derive_estimation_variables(
        population, sample, calibration_group_map, period, reference, cell_number
    )

    return estimation_data


def derive_estimation_variables(
    population_frame,
    sample,
    calibration_group_map,
    period,
    reference,
    cell_number,
    **config
):
    """
    Derive extra variables required for estimation.

    Parameters
    ----------
    population_frame: pd.DataFrame
        dataframe containing population frame
    sample: pd.DataFrame
        dataframe containing sample data
    calibration_group_map: pd.DataFrame
        dataframe containing map between cell number and calibration group
    period: Str
        the name of the period column
    cell_number: Str
        the name of the cell number column
    reference: Str
        the name of the reference column
    **config: Dict
       main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        population frame containing sampled column

    """
    population_frame.merge(calibration_group_map, on=[cell_number], how="left")
    # TODO: check if cell_no is the strata or if it should be dropped

    sample = sample[[reference, period]]
    sample["sampled"] = 1

    return population_frame.merge(sample, on=[reference, period], how="left").fillna(
        value={"sampled": 0}
    )
