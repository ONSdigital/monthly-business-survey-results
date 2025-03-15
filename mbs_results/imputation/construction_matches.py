import pandas as pd


def flag_construction_matches(dataframe, target, period, auxiliary, **kwargs):
    """
    Add flag to indicate whether the record has non-null target, period and
    auxiliary variables, and is therefore valid to use when calculating
    construction links

    Parameters
    ----------
    dataframe : pandas.DataFrame
    target : string
        name of column containing the target variable
    period : string
        name of column containing time period
    auxiliary : string
        name of column containing auxiliary information
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.
    Returns
    -------
    pandas.DataFrame
        dataframe with additional flag_construction_matches column
    """
    if f"imputation_flags_{target}" in dataframe.columns:
        print(dataframe[f"imputation_flags_{target}"])
        print(
            (dataframe[f"imputation_flags_{target}"] == "r")
            | (dataframe[f"imputation_flags_{target}"].isna())
        )
        dataframe["flag_construction_matches"] = pd.notna(
            dataframe[[target, period, auxiliary]]
        ).all(axis="columns") & (
            (dataframe[f"imputation_flags_{target}"].str.lower() == "r")
            | (dataframe[f"imputation_flags_{target}"].isna())
        )

    else:
        dataframe["flag_construction_matches"] = pd.notna(
            dataframe[[target, period, auxiliary]]
        ).all(axis="columns")

    return dataframe
