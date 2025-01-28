from mbs_results.staging.data_cleaning import convert_cell_number
from mbs_results.utilities.utils import read_colon_separated_file


def get_estimation_data(
    population_file,
    sample_file,
    period,
    population_column_names,
    sample_column_names,
    population_keep_columns,
    sample_keep_columns,
    calibration_group_map,
    reference,
    cell_number,
    **config
):
    """
    Get the input data required to run estimation.

    Parameters
    ----------
    population_file: pd.DataFrame
        file path to the folder containing the population frames
    sample_path: pd.DataFrame
        file path to the folder containing the sample data
    population_column_names: List[str]
        list of column names for the population frames
    sample_column_names: List[str]
        list of column names for the sample data
    population_keep_columns: List[str]
        list of names of columns to keep from population frame
    sample_keep_columns: List[str]
        list of names of columns to keep from sample
    calibration_group_map: pd.DataFrame
        dataframe containing map between cell number and calibration group
    period: Str
        the name of the period column
    reference: Str
        the name of the reference column
    cell_number: Str
        the name of the cell number column
    **config: Dict
       main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        population frame containing period and sampled columns.

    """
    population_df = read_colon_separated_file(population_file, population_column_names)

    population_df = population_df[population_keep_columns]

    sample_df = read_colon_separated_file(sample_file, sample_column_names)

    sample_df = sample_df[sample_keep_columns]

    estimation_data = derive_estimation_variables(
        population_df, sample_df, calibration_group_map, period, reference, cell_number
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

    population_frame = convert_cell_number(population_frame, cell_number)

    population_frame = population_frame.merge(
        calibration_group_map, on=[cell_number], how="left"
    )

    sample = sample.copy()[[reference, period]]
    sample["is_sampled"] = True

    return population_frame.merge(sample, on=[reference, period], how="left").fillna(
        value={"is_sampled": False}
    )
