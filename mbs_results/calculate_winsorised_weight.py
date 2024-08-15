import numpy as np
import pandas as pd



def calculate_winsorised_weight(
    df,
    group,
    period,
    aux,
    sampled,
    a_weight,
    g_weight,
    target_variable,
    predicted_unit_value,
    l_values,
    ratio_estimation_treshold,
    nw_ag_flag,
):
    """
    Calculate winsorised weight

    Parameters
    ----------
    df : pd.Dataframe
        Original dataframe
    group : str
        Column name containing group information (sic).
    period : str
        Column name containing time period.
    aux : str
        Column name containing auxiliary variable (x).
    sampled : str
        Column name indicating whether it was sampled or not -boolean.
    a_weight : str
        Column name containing the design weight.
    g_weight:str
        column name containing the g weight.
    target_variable : str
        Column name of the predicted target variable.
    predicted_unit_value: str
        column name containing the predicted unit value.
    l_values: str
        column name containing the l values as provided by methodology.
    ratio_estimation_treshold: str
        column name containing the previously calculated ratio estimation threshold.
    nw_ag_flag: str
        column name indicating whether it can't be winsorised-
        boolean (True means it can't be winsorised, False means it can).


    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame with a new column containing the winsorised weights.
    """

    df["w"] = df[a_weight] * df[g_weight]

    df["new_target"] = (df[target_variable] / df["w"]) + (
        df[ratio_estimation_treshold] - (df[ratio_estimation_treshold] / df["w"])
    )

    mask = df[target_variable] <= df[ratio_estimation_treshold]
    df["new_target_variable"] = np.where(mask, df[target_variable], df["new_target"])

    df["outlier_weight"] = df["new_target_variable"] / df[target_variable]

    df = df.drop(["w", "new_target"], axis=1)

    non_winsorised = (df[sampled] == 0) | (df[nw_ag_flag] == True)  # noqa: E712
    df["outlier_weight"] = df["outlier_weight"].mask(non_winsorised, np.nan)
    df["new_target_variable"] = df["new_target_variable"].mask(non_winsorised, np.nan)

    return df


def overwrite_l_values(
    df: pd.DataFrame,
    strata,
    question_no,
    l_values_col,
    l_values_overwrite_path,
    **config,
):
    """
    function to load the replacement l values data and overwrite
    default l values on main df

    Parameters
    ----------
    df: pd.DataFrame
        dataframe with l value column
    strata: str
        the name of the strata column
    question_no: str
        the name of the question number column
    l_values_col: str
        the name of the l value column
    l_values_path: str
        the location of the l values data used to overwrite existing values
    **config: Dict
        main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        dataframe with l values replaced as outlined in l_values_overwrite_path
        Additional flag column showing which values have been overwritten
    """
    l_values_overwrite = pd.read_csv(l_values_overwrite_path)

    l_values_overwrite[strata] = l_values_overwrite[strata].astype(int).astype(str)
    l_values_overwrite[question_no] = (
        l_values_overwrite[question_no].astype(int).astype(str)
    )
    # df[question_no] = df[question_no].astype('int')

    # l_values.set_index([strata, question_no], inplace=True)

    validate_l_values(df, l_values_overwrite, strata, question_no)

    merged = df.merge(
        l_values_overwrite,
        on=[strata, question_no],
        how="outer",
        suffixes=["", "_overwrite"],
    )

    l_values_overwrite = l_values_col + "_overwrite"

    merged["ovewrited_L_value"] = merged[l_values_overwrite].notna()
    merged.loc[merged[l_values_overwrite].notna(), l_values_col] = merged[
        l_values_overwrite
    ]
    merged.drop(columns=[l_values_overwrite], inplace=True)
    return merged


def validate_l_values(df, l_values_overwrite, strata, question_no):
    """
    Checks that all strata and question numbers in l values overwrite
    have values in the full dataframe

    Parameters
    ----------
    df: pd.DataFrame
        the main dataframe after preprocessing
    l_values_overwrite: pd.DataFrame
        the l values used to overwrite read in as a dataframe

    Raises
    ------
    ValueError
        ValueError if any combinations of strata and question number appear in
        the main dataset but not in the l values dataframe
    """
    df_temp = df.set_index([strata, question_no])
    l_values_temp = l_values_overwrite.set_index([strata, question_no])

    incorrect_ids = set(l_values_temp.index) - set(df_temp.index)

    if len(incorrect_ids) >= 1:
        string_ids = " ".join(
            [f"\n strata: {str(i[0])}, question_no: {str(i[1])}" for i in incorrect_ids]
        )

        raise ValueError(
            f"""There are strata and question_no combinations with no matching 
            l values: {string_ids}"""
        )


if __name__ == "__main__":

    df = pd.DataFrame(
        np.array(
            [
                ["1", "40", 202001, 1],
                ["1", "42", 202001, 2],
                ["2", "46", 202001, 2],
                ["2", "40", 202001, 1],
                ["2", "40", 202002, 1],
                ["2", "42", 202002, 1],
            ]
        ),
        columns=["strata", "question_no", "period", "l_value"],
    )
    # validate_l_values(df, l_values_overwrite, "strata", "question_no")
    output = overwrite_l_values(
        df, "strata", "question_no", "l_value", "l_values_overwrite.csv"
    )
    print(output)
