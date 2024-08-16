import pandas as pd


def replace_l_values(
    df: pd.DataFrame,
    strata: str,
    question_no: str,
    l_values_col: str,
    l_values_overwrite_path: str,
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

    l_values_overwrite[strata] = l_values_overwrite[strata].astype(str)
    l_values_overwrite[question_no] = l_values_overwrite[question_no].astype(str)

    validate_l_values(df, l_values_overwrite, strata, question_no)

    merged = df.merge(
        l_values_overwrite,
        on=[strata, question_no],
        how="outer",
        suffixes=["", "_overwrite"],
    )

    l_values_overwrite = l_values_col + "_overwrite"

    merged["replaced_l_value"] = merged[l_values_overwrite].notna()
    merged.loc[merged[l_values_overwrite].notna(), l_values_col] = merged[
        l_values_overwrite
    ]
    merged.drop(columns=[l_values_overwrite], inplace=True)
    return merged


def validate_l_values(
    df: pd.DataFrame,
    l_values_overwrite: pd.DataFrame,
    strata: str,
    question_no: str,
):
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
