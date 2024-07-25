from mbs_results.calculate_estimation_weights import (
    calculate_calibration_factor,
    calculate_design_weight,
)
from mbs_results.pre_processing_estimation import get_estimation_data
from mbs_results.validate_estimation import validate_estimation


def apply_estimation(df, reference, period, strata, group, auxiliary, **config):
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
    population_frame = get_estimation_data(**config)[
        reference, period, strata, group, auxiliary, "sampled"
    ]

    df = df.merge(population_frame, how="left", on=[reference, period])
    df = calculate_design_weight(df, **config)
    df = calculate_calibration_factor(df, **config)
    validate_estimation(df, **config)

    return df
