import pandas as pd

from mbs_results.staging.data_cleaning import convert_cell_number
from mbs_results.utilities.inputs import read_colon_separated_file


def get_estimation_data(
    population_file: str,
    sample_file: str,
    calibration_group_map: pd.DataFrame,
    convert_NI_GB_cells: bool,
    config: dict,
):
    """Get the input data required to run estimation.

    Parameters
    ----------
    population_file: pd.DataFrame
        File path to the folder containing the population frames.
    sample_file: pd.DataFrame
        File path to the folder containing the sample data.
    calibration_group_map: pd.DataFrame
        Dataframe containing map between cell number and calibration group.
    convert_NI_GB_cells: bool
        If True, will convert NI and GB cells to UK (convert_cell_number
        will be activated)
    config : dict
        Dictionary containing the following keys of interest:
        platform - either "s3" or "network"
        bucket_name - S3 bucket name for file storage. (optional)
        population_column_names: list of column names for the population frames
    sample_column_names: list of column names for the sample data
    population_keep_columns: list of names of columns to keep from population frame
    sample_keep_columns: list of names of columns to keep from sample
    calibration_group_map: dataframe containing map between cell number and
                            calibration group
    period: the name of the period column
    reference: the name of the reference column
    cell_number: the name of the cell number column

    Returns
    -------
    pd.DataFrame
        population frame containing period and sampled columns.

    """
    population_df = read_colon_separated_file(
        filepath=population_file,
        column_names=config["population_column_names"],
        keep_columns=config["population_keep_columns"],
        period=config["period"],
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )

    sample_df = read_colon_separated_file(
        filepath=sample_file,
        column_names=config["sample_column_names"],
        keep_columns=config["sample_keep_columns"],
        period=config["period"],
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )

    estimation_data = derive_estimation_variables(
        population_df,
        sample_df,
        config["period"],
        config["reference"],
        convert_NI_GB_cells,
        config["cell_number"],
        calibration_group_map,
    )

    return estimation_data


def derive_estimation_variables(
    population_frame: pd.DataFrame,
    sample: pd.DataFrame,
    period: str,
    reference: str,
    convert_NI_GB_cells: bool,
    cell_number: str,
    calibration_group_map: pd.DataFrame = None,
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
    period: Str
        the name of the period column
    convert_NI_GB_cells: bool
        If True, will convert NI and GB cells to UK (convert_cell_number
        will be activated)
    cell_number: Str
        the name of the cell number column
    reference: Str
        the name of the reference column
    calibration_group_map: pd.DataFrame
        dataframe containing map between cell number and calibration group
    **config: Dict
        main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        population frame containing sampled column

    """
    if convert_NI_GB_cells:
        population_frame = convert_cell_number(population_frame, cell_number)

    if calibration_group_map is not None:
        population_frame = population_frame.merge(
            calibration_group_map, on=[cell_number], how="left"
        )

    sample = sample.copy()[[reference, period]]
    sample["is_sampled"] = True

    population_frame = population_frame.merge(
        sample, on=[reference, period], how="left"
    )
    population_frame["is_sampled"] = (
        population_frame["is_sampled"].convert_dtypes().fillna(False)
    )
    return population_frame
