import numpy as np


def create_impute_flags(df, target, reference, strata, auxiliary):
    """
    function to create logical columns for each type of imputation
    Will feed into creating a column for imputation method

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing forward, backward predictive period columns (
        These columns are created by calling flag_matched_pair_merge forward
        and backwards)

    target : str
        column name containing target variable
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)
    auxiliary : str
        column name containing auxiliary data

    Returns
    -------
    pd.DataFrame
        Dataframe with four additional logical columns determining if target
        is a return (r_flag) can be imputed by forward imputation (fir_flag),
        backward imputation (bir_flag) or can be constructed (c_flag)
    """
    df["f_predictive_" + target + "_roll"] = df.groupby([reference, strata])[
        "f_predictive_" + target
    ].ffill()
    df["b_predictive_" + target + "_roll"] = df.groupby([reference, strata])[
        "b_predictive_" + target
    ].bfill()

    df["r_flag"] = df[target].notna()

    df["fir_flag"] = np.where(
        df["f_predictive_" + target + "_roll"].notna() & df[target].isna(), True, False
    )

    df["bir_flag"] = np.where(
        df["b_predictive_" + target + "_roll"].notna() & df[target].isna(), True, False
    )

    construction_conditions = df[target].isna() & df[auxiliary].notna()
    df["c_flag"] = np.where(construction_conditions, True, False)
    df.drop(
        ["f_predictive_target_variable_roll", "b_predictive_target_variable_roll"],
        axis=1,
        inplace=True,
    )

    return df
