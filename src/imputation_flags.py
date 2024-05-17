import numpy as np

from src.flag_and_count_matched_pairs import flag_matched_pair_merge


def create_impute_flags(df, target, reference, strata, auxiliary):
    """
    function to create logical columns for each type of imputation
    output columns are needed to create the string flag column for
    imputation methods.
    Function requires f_predictive and b_predictive columns produced
    by `flag_matched_pair` function

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
    for direction in ["f", "b"]:
        try:
            df["{}_predictive_{}".format(direction, target)]
        except KeyError:
            raise KeyError(
                "Dataframe needs column '{}_predictive_{}',\
                      run flag_matched_pair function first".format(
                    direction, target
                )
            )

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

    df = flag_matched_pair_merge(
        df=df,
        forward_or_backward="f",
        target="auxiliary",
        period="period",
        reference="reference",
        strata="strata",
    )

    df["f_predictive_" + auxiliary + "_roll"] = df.groupby([reference, strata])[
        "f_predictive_" + auxiliary
    ].ffill()
    fic_conditions = (
        df[target].isna() & df["f_predictive_" + auxiliary + "_roll"].notna()
    )
    df["fic_flag"] = np.where(fic_conditions, True, False)

    df.drop(
        [
            "f_predictive_" + target + "_roll",
            "b_predictive_" + target + "_roll",
            "f_predictive_" + auxiliary,
            "f_predictive_" + auxiliary + "_roll",
            "f_matched_pair_" + auxiliary,
        ],
        axis=1,
        inplace=True,
    )

    return df


def generate_imputation_flag_string(df):
    imputation_flag_conditions = [
        df["r_flag"],
        ~df["r_flag"] & df["fir_flag"],
        ~df["r_flag"] & ~df["fir_flag"] & df["bir_flag"],
        ~df["r_flag"] & ~df["fir_flag"] & ~df["bir_flag"] & df["fic_flag"],
        ~df["r_flag"]
        & ~df["fir_flag"]
        & ~df["bir_flag"]
        & ~df["fic_flag"]
        & df["c_flag"],
    ]
    flags = ["r", "fir", "bir", "fic", "c"]
    df["imputation_flag"] = np.select(
        imputation_flag_conditions, flags, default="error"
    )

    return df