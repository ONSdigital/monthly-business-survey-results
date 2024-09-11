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
        DataFrame with group, period, f_count and b_count
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
    groups: list,
    link_columns: list,
    count_columns: list,
    imputed_value: str,
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
    groups : list

    link_columns : list

    count_columns : list

    imputed_value: str
        name of column in dataframe containing imputed_value variable
    selected_periods: list
        list containing periods to include in output

    Returns
    -------
    dataframe filtered by date, containing imputation_value, pivoted by imputation type
    and grouped by sic, cell, question and imputation type.

    """
    if selected_periods is not None:
        df = df.query("{} in {}".format(groups[0], selected_periods))

    links_df = df.melt(
        id_vars=groups + [imputed_value],
        value_vars=link_columns,
        var_name="link_type",
        value_name="imputation_link",
    )

    link_type_map = dict(zip(link_columns, ["F", "B", "C"]))
    links_df["link_type"] = links_df["link_type"].map(link_type_map)

    counts_df = df.melt(
        id_vars=groups,
        value_vars=count_columns,
        var_name="link_type_count",
        value_name="count",
    )

    link_type_map_count = dict(zip(count_columns, ["F", "B", "C"]))
    counts_df["link_type_count"] = counts_df["link_type_count"].map(link_type_map_count)

    merged_df = pd.merge(
        links_df,
        counts_df,
        how="outer",
        left_on=groups + ["link_type"],
        right_on=groups + ["link_type_count"],
    )

    merged_df.drop_duplicates(inplace=True)
    merged_df.drop(["link_type_count"], axis=1, inplace=True)

    merged_df = merged_df.groupby(groups + ["link_type"], as_index=False).agg(
        {imputed_value: "sum", "count": "first", "imputation_link": "first"}
    )

    sorting_order = {"F": 1, "B": 2, "C": 3}
    merged_df["sort_column"] = merged_df["link_type"].map(sorting_order)

    merged_df = merged_df.sort_values(groups + ["sort_column"])

    merged_df.drop("sort_column", axis=1, inplace=True)

    merged_df.reset_index(drop=True, inplace=True)

    merged_df = merged_df[
        groups
        + [
            "imputation_link",
            "link_type",
            "count",
            imputed_value,
        ]
    ]

    return merged_df
