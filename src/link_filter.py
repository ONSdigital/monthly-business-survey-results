import pandas as pd

# TODO: Extend function to receive multiple df with *df_with_filters


def flag_rows_to_ignore(
    df: pd.DataFrame, df_with_filters: pd.DataFrame
) -> pd.DataFrame:
    """
    Add a new column bool column named ignore_from_link to df
    having as TRUE the observations defined in df_with_filters.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    df_with_filters : pd.DataFrame
        Dataframe with observations which should be flagged in the original
        dataframe.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with a bool column containing the flags.

    """

    if not set(df_with_filters.columns).issubset(df.columns):

        raise ValueError(
            f"""df_with_filters has these columns {list(df_with_filters)} while
            df has these columns {list(df)}, please
            double check the column names."""
        )

    # TODO: Check if values to be ignored exist

    df = df.set_index(list(df_with_filters))

    df_with_filters = df_with_filters.set_index(list(df_with_filters))

    df["ignore_from_link"] = df.index.isin(df_with_filters.index)

    df = df.reset_index()

    # TODO: Consider what should be logged and reroute print to logs
    print("These values were flagged:\n", df.loc[df["ignore_from_link"] is True])

    return df
