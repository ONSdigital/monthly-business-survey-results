import warnings

import pandas as pd

from mbs_results.estimation.apply_estimation import apply_estimation

# from mbs_results.staging.data_cleaning import correct_values


def estimate(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Apply the estimation method to the given dataframe

    Parameters
    ----------
    df : pd.DataFrame
        post imputation dataframe
    config : dict
        main config file

    Returns
    -------
    pd.DataFrame
        returns post estimation dataframe
    """
    warnings.warn("Estimate is slowest stage")
    estimate_df = apply_estimation(config)
    # estimate_df = correct_values()
    estimate_df = estimate_df.drop(columns=["cell_no", "frotover", config["sic"]])
    post_estimate = pd.merge(
        df, estimate_df, how="left", on=[config["period"], config["reference"]]
    )

    return post_estimate
