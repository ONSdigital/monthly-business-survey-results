import numpy as np
import pandas as pd


def create_impute_flags(
    df: pd.DataFrame,
    target: str,
    period: str,
    reference: str,
    strata: str,
    auxiliary: str,
    predictive_auxiliary: str,
    time_difference=1,
    **kwargs
):
    """
    function to create logical columns for each type of imputation
    output columns are needed to create the string flag column for
    imputation methods.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing forward, backward predictive period columns (
        These columns are created by calling flag_matched_pair forward
        and backwards)
    target : str
        Column name containing target variable.
    period: str
        Column name containing date variable.
    reference : str
        Column name containing business reference id.
    strata : str
        Column name containing strata information (sic).
    auxiliary : str
        Column name containing auxiliary data.
    predictive_auxiliary: str
        Column name containing predictive auxiliary data, this is created,
        by flag_matched_pair_merge function.
    time_difference: int
        lookup distance for matched pairs
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.

    Returns
    -------
    pd.DataFrame
        Dataframe with four additional logical columns determining if target
        is a return (r_flag) can be imputed by forward imputation (fir_flag),
        backward imputation (bir_flag) or can be constructed (c_flag)
    """

    df.sort_values([reference, strata, period], inplace=True)

    df["r_flag"] = df[target].notna()

    df["fir_flag"] = flag_rolling_impute(df, 1, strata, reference, target, period)

    df["bir_flag"] = flag_rolling_impute(df, 1, strata, reference, target, period)

    construction_conditions = df[target].isna() & df[auxiliary].notna()
    df["c_flag"] = np.where(construction_conditions, True, False)

    df["fic_flag"] = flag_rolling_impute(df, 1, strata, reference, auxiliary, period)

    df.drop

    return df


def flag_rolling_impute(df, time_difference, strata, reference, target, period):
    """
    Function to create logical values for whether rolling imputation can be done.
    Used to account for gaps of over one month when imputing.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing forward, backward predictive period columns (
        These columns are created by calling flag_matched_pair forward
        and backwards)
    time_difference: int
        lookup distance for matched pairs
    target : str
        Column name containing target variable.
    period: str
        Column name containing date variable.
    reference : str
        Column name containing business reference id.
    strata : str
        Column name containing strata information (sic).

    Returns
    -------
    pd.Series
    """

    return (
        df.groupby([strata, reference])
        .shift(time_difference)[target]
        .notnull()
        .mul(
            df[period] - pd.DateOffset(months=time_difference)
            == df.shift(time_difference)[period]
        )
    )


def generate_imputation_marker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to add column containing the a string indicating the method of
    imputation to use following the hierarchy in specifications

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing logical columns produced by `create_imputation_flags`
        (r_flag, fir_flag, bir_flag, fic_flag and c_flag)


    Returns
    -------
    pd.DataFrame
        Dataframe with additional column containing imputation marker
        i.e. the type of imputation method that should be used to fill
        missing returns.
    """

    imputation_markers_and_conditions = {
        "r": df["r_flag"],
        "fir": ~df["r_flag"] & df["fir_flag"],
        "bir": ~df["r_flag"] & ~df["fir_flag"] & df["bir_flag"],
        "fic": ~df["r_flag"] & ~df["fir_flag"] & ~df["bir_flag"] & df["fic_flag"],
        "c": ~df["r_flag"]
        & ~df["fir_flag"]
        & ~df["bir_flag"]
        & ~df["fic_flag"]
        & df["c_flag"],
    }

    df["imputation_marker"] = np.select(
        imputation_markers_and_conditions.values(),
        imputation_markers_and_conditions.keys(),
        default="error",
    )

    return df
