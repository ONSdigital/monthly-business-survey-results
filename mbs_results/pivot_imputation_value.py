import numpy as np
import pandas as pd


def merge_counts(input_df: pd.DataFrame, count_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns input data with f_count and b_count merged on.

    Parameters
    ----------
    input_df : pd.DataFrame
        Reference dataframe with identifier, date, sic, cell, forward, backward, construction, question,imputed_value
    count_df : pd.DataFrame
        DataFrame with group, period, flag_1 and flag_2.
    input_cell :
        name of column in input_df dataframe containing cell variable
    count_cell :
        name of column in count_df dataframe containing cell variable
    input_date :
        name of column in input_df dataframe containing date variable
    count_date :
        name of column in count_df dataframe containing date variable
    Returns
    Returns
    -------
    Dataframe resulting from the left-join of df_2 and df_1 (after renaming columns), on 'cell' and 'date'.
    """
    count_df = count_df.rename(
        columns={
            "group": "cell",
            "period": "date",
            "flag_1": "f_count",
            "flag_2": "b_count",
        }
    )
    df_merge = pd.merge(input_df, count_df, how="left", on=["cell", "date"])
    df_merge["identifier"] = df_merge["identifier"].astype(int)
    return df_merge


def pivot_imputation_value(
    df: pd.DataFrame,
    identifier: str,
    date: str,
    sic: str,
    cell: str,
    forward: str,
    backward: str,
    construction: str,
    question: str,
    imputed_value: str,
    f_count: str,
    b_count: str,
) -> pd.DataFrame:

    """
    Returning dataframe containing imputation_value, filtered by date, pivoted by imputation type
    and grouped by sic, cell, question and imputation type.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe with domain, a_weights, o_weights, and g_weights
    identifier : str
        name of column in dataframe containing identifier variable
    date : str
        name of column in dataframe containing period variable
    sic : str
        name of column in dataframe containing domain variable
    cell : str
        name of column in dataframe containing question code variable
    forward : str
        name of column in dataframe containing predicted value variable
    backward : str
        name of column in dataframe containing imputation marker variable
    construction : str
        name of column in dataframe containing a_weight variable
    question : str
        name of column in dataframe containing o_weight variable
    imputed_value: str
        name of column in dataframe containing imputed_value variable
    f_count: str,
        name of column in dataframe containing f_count variable
    b_count: str,
        name of column in dataframe containing b_count variable

    Returns
    -------
    dataframe filtered by date, containing imputation_value, pivoted by imputation type
    and grouped by sic, cell, question and imputation type.

    """

    df = df[df[date] == 202001]

    df1 = df.melt(
        id_vars=[date, sic, cell, question, imputed_value],
        value_vars=[forward, backward, construction],
        var_name="link_type",
        value_name="imputation_link",
    )

    link_type_map = {forward: "F", backward: "B", construction: "C"}
    df1["link_type"] = df1["link_type"].map(link_type_map)

    df2 = df.melt(
        id_vars=[date, sic, cell, question],
        value_vars=[f_count, b_count],
        var_name="link_type_count",
        value_name="count",
    )

    link_type_map_count = {f_count: "F", b_count: "B"}
    df2["link_type_count"] = df2["link_type_count"].map(link_type_map_count)

    merged_df = pd.merge(
        df1,
        df2,
        how="outer",
        left_on=[date, sic, cell, question, "link_type"],
        right_on=[date, sic, cell, question, "link_type_count"],
    )

    merged_df.drop_duplicates(inplace=True)
    merged_df.drop(["link_type_count"], axis=1, inplace=True)

    merged_df = merged_df.groupby(
        [date, sic, cell, question, "link_type"], as_index=False
    ).agg({imputed_value: "sum", "count": "first", "imputation_link": "first"})

    sorting_order = {"F": 1, "B": 2, "C": 3}
    merged_df["sort_column"] = merged_df["link_type"].map(sorting_order)

    merged_df = merged_df.sort_values([date, sic, cell, question, "sort_column"])

    merged_df.drop("sort_column", axis=1, inplace=True)

    merged_df.reset_index(drop=True, inplace=True)

    merged_df = merged_df[
        [
            date,
            sic,
            cell,
            question,
            "imputation_link",
            "link_type",
            "count",
            imputed_value,
        ]
    ]

    return merged_df
