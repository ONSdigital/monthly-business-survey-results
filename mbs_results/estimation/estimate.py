import pandas as pd

from mbs_results.estimation.apply_estimation import apply_estimation
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.utils import get_versioned_filename


def estimate(
    df: pd.DataFrame, method: str, convert_NI_GB_cells: bool, config: dict
) -> pd.DataFrame:
    """
    Apply the estimation method to the given dataframe

    Parameters
    ----------
    df : pd.DataFrame
        post imputation dataframe
    method : str
        Method to be applied, accepted values are `separate` and `combined`
        For separate, calibration factor is calculated at strata variable
        For combined ratio,calibration factor is calculated at calibration
        group, calibration group mapping must be supplied via a csv file.
    convert_NI_GB_cells: bool
        If True, will convert NI and GB cells to UK (convert_cell_number
        will be activated)
    config : dict
        main config file

    Returns
    -------
    pd.DataFrame
        returns post estimation dataframe
    """
    estimate_df = apply_estimation(method, convert_NI_GB_cells, config)
    estimate_df = estimate_df.drop(columns=["cell_no", "frotover", config["sic"]])
    # Dropping region column if already exists in imputation, to prevent duplicate
    # region columns in construction.
    if config["region"] in df.columns:
        estimate_df = estimate_df.drop(columns="region")

    post_estimate = pd.merge(
        df, estimate_df, how="left", on=[config["period"], config["reference"]]
    )

    # export on demand
    if config["debug_mode"]:

        estimate_filename = get_versioned_filename("estimation_output", config)

        write_csv_wrapper(
            post_estimate,
            config["output_path"] + estimate_filename,
            config["platform"],
            config["bucket"],
            index=False,
        )

    return post_estimate
