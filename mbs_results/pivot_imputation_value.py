import pandas as pd


def merge_counts(
    input_df: pd.DataFrame,
    count_df: pd.DataFrame,
    input_cell: str,
    count_cell: str,
    input_date: str,
    count_date: str,
    identifier: str,
) -> pd.DataFrame:
    """
    Returns input data with f_count and b_count merged on.

    Parameters
    ----------
    input_df : pd.DataFrame
        Reference dataframe with identifier, date, sic, cell, forward, backward,
        construction, question, imputed_value
    count_df : pd.DataFrame
        DataFrame with group, period, flag_1 and flag_2.
    input_cell : str
        name of column in input_df dataframe containing cell variable
    count_cell : str
        name of column in count_df dataframe containing cell variable
    input_date : str
        name of column in input_df dataframe containing date variable
    count_date : str
        name of column in count_df dataframe containing date variable
    identifier : str
        name of column in input_df containing identifier variable

    Returns
    -------
    Dataframe resulting from the left-join of input_df and count_df on the cell and
    date columns.
    """
    count_df = count_df.rename(
        columns={
            "flag_1": "f_count",
            "flag_2": "b_count",
        }
    )
    df_merge = pd.merge(
        input_df,
        count_df,
        how="left",
        left_on=[input_cell, input_date],
        right_on=[count_cell, count_date],
    ).astype({identifier: "int"})

    return df_merge.drop(columns=[count_cell, count_date])


def pivot_imputation_value(
    df: pd.DataFrame,
    identifier: str,
    date: str,
    cell: str,
    forward: str,
    backward: str,
    construction: str,
    question: str,
    imputed_value: str,
    f_count: str,
    b_count: str,
    selected_periods: list = None,
) -> pd.DataFrame:

    """
    Returning dataframe containing imputation_value, filtered by date, pivoted by
    imputation type and grouped by sic, cell, question and imputation type.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe containing links, count values, and imputed values
        by identifier, cell, date, and question
    identifier : str
        name of column in dataframe containing identifier variable
    date : str
        name of column in dataframe containing date variable
    cell : str
        name of column in dataframe containing cell variable
    forward : str
        name of column in dataframe containing forward link variable
    backward : str
        name of column in dataframe containing backward link variable
    construction : str
        name of column in dataframe containing construction link variable
    question : str
        name of column in dataframe containing question code variable
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
    if selected_periods is not None:
        df = df.query("{} in {}".format(date, selected_periods))

    links_df = df.melt(
        id_vars=[date, cell, question, imputed_value],
        value_vars=[forward, backward, construction],
        var_name="link_type",
        value_name="imputation_link",
    )

    link_type_map = {forward: "F", backward: "B", construction: "C"}
    links_df["link_type"] = links_df["link_type"].map(link_type_map)

    counts_df = df.melt(
        id_vars=[date, cell, question],
        value_vars=[f_count, b_count],
        var_name="link_type_count",
        value_name="count",
    )

    link_type_map_count = {f_count: "F", b_count: "B"}
    counts_df["link_type_count"] = counts_df["link_type_count"].map(link_type_map_count)

    merged_df = pd.merge(
        links_df,
        counts_df,
        how="outer",
        left_on=[date, cell, question, "link_type"],
        right_on=[date, cell, question, "link_type_count"],
    )

    merged_df.drop_duplicates(inplace=True)
    merged_df.drop(["link_type_count"], axis=1, inplace=True)

    merged_df = merged_df.groupby(
        [date, cell, question, "link_type"], as_index=False
    ).agg({imputed_value: "sum", "count": "first", "imputation_link": "first"})

    sorting_order = {"F": 1, "B": 2, "C": 3}
    merged_df["sort_column"] = merged_df["link_type"].map(sorting_order)

    merged_df = merged_df.sort_values([date, cell, question, "sort_column"])

    merged_df.drop("sort_column", axis=1, inplace=True)

    merged_df.reset_index(drop=True, inplace=True)

    merged_df = merged_df[
        [
            date,
            cell,
            question,
            "imputation_link",
            "link_type",
            "count",
            imputed_value,
        ]
    ]

    return merged_df
