import warnings

import pandas as pd

from mbs_results.estimation.apply_estimation import apply_estimation


def estimate(df: pd.DataFrame, config):
    """
    Applys the estimation method to the given dataframe

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    config : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    warnings.warn("Apply estimation loads data using universe files")
    estimate_df = apply_estimation(**config)
    estimate_df = estimate_df.drop(columns=["cell_no", "frotover"])
    post_estimate = pd.merge(df, estimate_df, how="left", on=["period", "reference"])

    return post_estimate
