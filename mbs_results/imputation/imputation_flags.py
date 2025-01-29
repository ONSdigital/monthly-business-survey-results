import numpy as np
import pandas as pd


def generate_imputation_marker(
    df: pd.DataFrame,
    target: str,
    period: str,
    reference: str,
    strata: str,
    auxiliary: str,
    back_data_period: str,
    time_difference=1,
    **kwargs,
) -> pd.DataFrame:
    """
    Function to add column containing the a string indicating the method of
    imputation to use following the hierarchy in specifications
        Implements the following hierarchy:
        r - mc - fir - bir - fimc - fic - c

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
    back_data_period : pd.Timestamp
        Time period used as the back data period. This periods data
        should not be changed
    time_difference: int, Optional
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
        flags = ["r", "mc", "fir", "bir", "fimc", "fic", "c"]
        # Check order from Specs
    else:
        flags = ["r", "fir", "bir", "fic", "c"]

    create_imputation_logical_columns(
        df,
        target,
        period,
        reference,
        strata,
        auxiliary,
        back_data_period,
        time_difference,
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
    back_data_period: str,
    time_difference: int = 1,
):
    """
    function to create logical columns for each type of imputation
    output columns are needed to create the string flag column for
    imputation methods.
    No order is needed for imputation logical columns, this is
    handled in the generate_imputation_marker function.

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

    if f"imputation_flags_{target}" in df.columns:
        # Case where back data is present
        backdata_r_mask = df[f"backdata_flags_{target}"] == "r"
        backdata_fir_mask = df[f"backdata_flags_{target}"] == "fir"
        backdata_fimc_mask = df[f"backdata_flags_{target}"] == "fimc"
        backdata_c_mask = df[f"backdata_flags_{target}"] == "c"
        backdata_fic_mask = df[f"backdata_flags_{target}"] == "fic"
        backdata_bir_mask = df[f"backdata_flags_{target}"] == "bir"
        prior_month_backdata_bir_mask = df[f"backdata_flags_{target}"].shift(1) == "bir"

    else:
        df["is_backdata"] = df[reference] != df[reference]
        backdata_r_mask = df[reference] != df[reference]
        backdata_fir_mask = df[reference] != df[reference]
        backdata_fimc_mask = df[reference] != df[reference]
        backdata_c_mask = df[reference] != df[reference]
        backdata_fic_mask = df[reference] != df[reference]
        backdata_bir_mask = False
        prior_month_backdata_bir_mask = False

    # if target na but not back data period OR if backdata flag is 'r'
    df[f"r_flag_{target}"] = (df[target].notna() & ~df["is_backdata"]) | backdata_r_mask

    if f"{target}_man" in df.columns:
        df[f"mc_flag_{target}"] = df[f"{target}_man"].notna()

    df[f"fir_flag_{target}"] = (
        flag_rolling_impute(df, time_difference, strata, reference, target, period)
        & ~df["is_backdata"]
    ) | backdata_fir_mask

    df[f"bir_flag_{target}"] = (
        (
            flag_rolling_impute(df, -time_difference, strata, reference, target, period)
            & ~df["is_backdata"]
        )
        | backdata_r_mask
        | backdata_bir_mask
    )

    if f"{target}_man" in df.columns:
        df[f"fimc_flag_{target}"] = (
            flag_rolling_impute(
                df, time_difference, strata, reference, f"{target}_man", period
            )
            | backdata_fimc_mask
        )

        df = imputation_overlaps_mc(df, target, reference, strata)

    construction_conditions = (
        df[target].isna() & df[auxiliary].notna() & ~df["is_backdata"]
    ) | backdata_c_mask
    df[f"c_flag_{target}"] = np.where(construction_conditions, True, False)

    df[f"fic_flag_{target}"] = (
        flag_rolling_impute(df, time_difference, strata, reference, auxiliary, period)
        | backdata_fic_mask
    ) & ~(prior_month_backdata_bir_mask)

    return df


def imputation_overlaps_mc(df, target, reference, strata):
    """
    function which checks and breaks a chain of imputations if a
    manual construction is present
    e.g. r, fir, mc, fimc or c, mc, bir, r

    Parameters
    ----------
    df : pd.Dataframe
        dataframe
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
    pd.Dataframe
        Original dataframe with updated columns for forward and backwards
        imputation boolean columns
    """

    for column in ["back_impute_overlaps_mc", "forward_impute_overlaps_mc"]:
        direction_single_string = column[0]
        imputation_marker_column = direction_single_string + f"ir_flag_{target}"
        df[column] = np.where(
            df[imputation_marker_column] & df[f"mc_flag_{target}"], False, None
        )
        if direction_single_string == "b":
            df[column] = (
                df.groupby([strata, reference])[column].bfill().astype(bool)
            ).fillna(True)
        elif direction_single_string == "f":
            df[column] = (
                df.groupby([strata, reference])[column].ffill().astype(bool)
            ).fillna(True)

        df[imputation_marker_column] = df[imputation_marker_column] & df[column]
        df.drop(
            columns=[column],
            inplace=True,
        )
    return df


def flag_rolling_impute(
    df: pd.DataFrame,
    time_difference: int,
    strata: str,
    reference: str,
    target: str,
    period: str,
):
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
    # Fill group tells when forward/backward filling should apply
    # If there are mannual constuctions we need to account and create a new
    # fill group, if they don't exist then forward or backward fill (fill group)
    # should be applied for references with same strata without date gap

    mc_exists_rule = (
        (df[f"{target}_man"].notna()) if f"{target}_man" in df.columns else False
    )

    df["fill_group"] = (
        (df[period] - pd.DateOffset(months=1) != df.shift(1)[period])
        | (df[strata].diff(1) != 0)
        | (df[reference].diff(1) != 0)
        | mc_exists_rule
    ).cumsum()

    if time_difference < 0:
        boolean_column = (
            df.groupby(["fill_group"])[target]
            .bfill()
            .notnull()
            .mul(df["fill_group"] == df.shift(time_difference)["fill_group"])
        )

    elif time_difference > 0:
        boolean_column = (
            df.groupby(["fill_group"])[target]
            .ffill()
            .notnull()
            .mul(df["fill_group"] == df.shift(time_difference)["fill_group"])
        )

    df.drop(columns="fill_group", inplace=True)

    return boolean_column
