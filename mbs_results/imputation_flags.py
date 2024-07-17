import numpy as np
import pandas as pd


def generate_imputation_marker(
    df: pd.DataFrame,
    target: str,
    period: str,
    reference: str,
    strata: str,
    auxiliary: str,
    time_difference=1,
    **kwargs,
) -> pd.DataFrame:
    """
    Function to add column containing the a string indicating the method of
    imputation to use following the hierarchy in specifications

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing target, auxiliary and manual construction columns
        and containing forward, backward predictive period columns (
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
    time_difference: int
        lookup distance for matched pairs
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.

    Returns
    -------
    pd.DataFrame
        Dataframe with additional column containing imputation marker
        i.e. the type of imputation method that should be used to fill
        missing returns.
    """
    if f"{target}_man" in df.columns:
        flags = ["r", "mc", "fir", "bir", "fimc", "c", "fic"]
    else:
        flags = ["r", "fir", "bir", "c", "fic"]

    create_imputation_logical_columns(
        df, target, period, reference, strata, auxiliary, time_difference
    )

    select_cols = [f"{i}_flag_{target}" for i in flags]
    first_condition_met = [np.where(i)[0][0] for i in df[select_cols].values]
    df[f"imputation_flags_{target}"] = [flags[i] for i in first_condition_met]
    df.drop(columns=select_cols, inplace=True)

    return df


def create_imputation_logical_columns(
    df: pd.DataFrame,
    target: str,
    period: str,
    reference: str,
    strata: str,
    auxiliary: str,
    time_difference=1,
    **kwargs,
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
    time_difference: int
        lookup distance for matched pairs

    Returns
    -------
    pd.DataFrame
        Dataframe with four additional logical columns determining if target
        is a return (r_flag) can be imputed by forward imputation (fir_flag),
        backward imputation (bir_flag) or can be constructed (c_flag)
    """

    df.sort_values([reference, strata, period], inplace=True)

    df[f"r_flag_{target}"] = df[target].notna()

    if f"{target}_man" in df.columns:
        df[f"mc_flag_{target}"] = df[f"{target}_man"].notna()

    df[f"fir_flag_{target}"] = flag_rolling_impute(
        df, time_difference, strata, reference, target, period
    )

    df[f"bir_flag_{target}"] = flag_rolling_impute(
        df, -time_difference, strata, reference, target, period
    )

    if f"{target}_man" in df.columns:
        df[f"fimc_flag_{target}"] = flag_rolling_impute(
            df, time_difference, strata, reference, f"{target}_man", period
        )

        df["back_impute_overlaps_mc"] = np.where(
            df[f"bir_flag_{target}"] & df[f"mc_flag_{target}"], False, None
        )
        df["back_impute_overlaps_mc"] = (
            df.groupby([strata, reference])["back_impute_overlaps_mc"].fillna(
                method="bfill"
            )
        ).fillna(True)
        df[f"bir_flag_{target}"] = (
            df[f"bir_flag_{target}"] & df["back_impute_overlaps_mc"]
        )

        df["forward_impute_overlaps_mc"] = np.where(
            df[f"fir_flag_{target}"] & df[f"mc_flag_{target}"], False, None
        )
        df["forward_impute_overlaps_mc"] = (
            df.groupby([strata, reference])["forward_impute_overlaps_mc"].fillna(
                method="ffill"
            )
        ).fillna(True)

        df[f"fir_flag_{target}"] = (
            df[f"fir_flag_{target}"] & df["forward_impute_overlaps_mc"]
        )
        df.drop(
            columns=["back_impute_overlaps_mc", "forward_impute_overlaps_mc"],
            inplace=True,
        )

    construction_conditions = df[target].isna() & df[auxiliary].notna()
    df[f"c_flag_{target}"] = np.where(construction_conditions, True, False)

    df[f"fic_flag_{target}"] = flag_rolling_impute(
        df, time_difference, strata, reference, auxiliary, period
    )

    return df


def check_imputation_overlaps_manual_construction(
    df, target, strata, reference, time_difference
):
    if time_difference < 0:
        direction = "backwards"
        fillmethod = "bfill"
    elif time_difference > 0:
        direction = "forwards"
        fillmethod = "ffill"
    direction_flag = direction[0]

    df[f"{direction}_impute_overlaps_mc"] = np.where(
        df[f"{direction_flag}ir_flag_{target}"] & df[f"mc_flag_{target}"], False, None
    )
    df[f"{direction}_impute_overlaps_mc"] = (
        df.groupby([strata, reference])["{direction}_impute_overlaps_mc"].fillna(
            method=fillmethod
        )
    ).fillna(True)


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

    if time_difference < 0:
        fillmethod = "bfill"
    elif time_difference > 0:
        fillmethod = "ffill"

    return (
        df.groupby([strata, reference])[target]
        .fillna(method=fillmethod)
        .notnull()
        .mul(
            df[period] - pd.DateOffset(months=time_difference)
            == df.shift(time_difference)[period]
        )
        .mul(df[target].isna())
    )
