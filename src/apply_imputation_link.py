import pandas as pd

df = pd.read_csv(
    "monthly-business-survey-results/tests/apply_imputation_link.csv"
).drop(columns="imputed_value")

df.sort_values(["strata", "reference", "period"], inplace=True)

grouped_target = df.groupby(["strata", "reference"])["target"]

df["fir"] = grouped_target.ffill() * df["cumulative_forward_imputation_link"]
df["bir"] = grouped_target.bfill() * df["cumulative_backward_imputation_link"]

df.loc[df["imputation_marker"] == "C", "imputed_value"] = (
    df["construction_link"] * df["auxiliary_variable"]
)

df["fic"] = (
    df.groupby(["strata", "reference"])["imputed_value"].ffill()
    * df["cumulative_forward_imputation_link"]
)

marker_column_mapping = {"FIR": "fir", "BIR": "bir", "FIC": "fic"}


def add_to_imputation_column(df, marker, column):
    df.loc[df["imputation_marker"] == marker, "imputed_value"] = df[column]
    return df


for marker, column in marker_column_mapping.items():
    df = add_to_imputation_column(df, marker, column)

df.drop(columns=marker_column_mapping.values(), inplace=True)


def get_impute(
    dataframe,
    forward_or_backward,
    strata,
    reference,
    target,
    period,
    imputation_link,
    time_difference=1,
):
    """
    Create cumulative imputation links for multiple consecutive periods
    without a return.

    Parameters
    ----------
    dataframe : pandas.DataFrame
    forward_or_backward: str
        either f or b for forward or backward method

    strata : str
        column name containing strata information (sic)
    reference : str
        column name containing business reference id
    target : str
        column name containing target variable
    period : str
        column name containing time period
    imputation_link : string
        column name containing imputation links
    time_difference : int
        time difference between predictive and target period in months

    Returns
    -------
    pandas.DataFrame
        dataframe with imputation_group and
        cumulative_forward/backward_imputation_link column
    """

    dataframe.sort_values([strata, reference, period], inplace=True)

    return dataframe[["imputation_group", "cumulative_" + imputation_link]]
