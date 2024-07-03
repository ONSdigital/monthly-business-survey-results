import numpy as np
import pandas as pd


def create_impute_flags(
    df: pd.DataFrame,
    target: str,
    reference: str,
    strata: str,
    auxiliary: str,
    predictive_auxiliary: str,
):
    """
    function to create logical columns for each type of imputation
    output columns are needed to create the string flag column for
    imputation methods.
    Function requires f_predictive and b_predictive columns produced
    by `flag_matched_pair` function.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing forward, backward predictive period columns (
        These columns are created by calling flag_matched_pair_merge forward
        and backwards)

    target : str
        Column name containing target variable.
    reference : str
        Column name containing business reference id.
    strata : str
        Column name containing strata information (sic).
    auxiliary : str
        Column name containing auxiliary data.
    predictive_auxiliary: str
        Column name containing predictive auxiliary data, this is created,
        by flag_matched_pair_merge function.

    Returns
    -------
    pd.DataFrame
        Dataframe with four additional logical columns determining if target
        is a return (r_flag) can be imputed by forward imputation (fir_flag),
        backward imputation (bir_flag) or can be constructed (c_flag)
    """
    for direction in ["f", "b"]:
        try:
            df["{}_predictive_{}".format(direction, target)]
        except KeyError:
            raise KeyError(
                "Dataframe needs column '{}_predictive_{}',".format(direction, target)
                + " run flag_matched_pair function first"
            )
    forward_target_roll = "f_predictive_" + target + "_roll"
    backward_target_roll = "b_predictive_" + target + "_roll"
    forward_aux_roll = "f_predictive_" + auxiliary + "_roll"

    df[forward_target_roll] = df.groupby([reference, strata])[
        "f_predictive_" + target
    ].ffill()

    df[backward_target_roll] = df.groupby([reference, strata])[
        "b_predictive_" + target
    ].bfill()

    df["r_flag"] = df[target].notna()

    df["fir_flag"] = np.where(
        df[forward_target_roll].notna() & df[target].isna(), True, False
    )

    df["bir_flag"] = np.where(
        df[backward_target_roll].notna() & df[target].isna(), True, False
    )

    construction_conditions = df[target].isna() & df[auxiliary].notna()
    df["c_flag"] = np.where(construction_conditions, True, False)

    df[forward_aux_roll] = df.groupby([reference, strata])[predictive_auxiliary].ffill()

    fic_conditions = df[target].isna() & df[forward_aux_roll].notna()
    df["fic_flag"] = np.where(fic_conditions, True, False)

    df.drop(
        [
            forward_target_roll,
            backward_target_roll,
            forward_aux_roll,
            predictive_auxiliary,
        ],
        axis=1,
        inplace=True,
    )

    return df


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
