import numpy as np


def create_impute_flags(df, target, reference, strata, auxiliary):
    temp_df = df.copy()
    df["f_predictive_" + target + "_roll"] = temp_df.groupby([reference, strata])[
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
