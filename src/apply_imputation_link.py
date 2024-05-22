import pandas as pd

df = pd.read_csv(
    "monthly-business-survey-results/tests/apply_imputation_link.csv"
).drop(columns="imputed_value")


def create_and_merge_imputation_values(
    df, imputation_class, reference, period, marker, combined_imputation
):
    """
    Loop through different imputation types and merge the results according
    to an imputation marker column

    Parameters
    ----------
    df : pandas.DataFrame
    imputation_class: str
        column name for the variable that defines the imputation class
    reference : str
        column name for the reference
    period : str
        column name for the period
    marker : str
        column name containing a marker to indicate the type of imputation required
    combined_imputation : str
        column name for the combined imputation types according to the imputation marker

    Returns
    -------
    pandas.DataFrame
        dataframe with imputation values defined by the imputation marker
    """
    # constructed has to come first to use the result for forward impute from contructed
    imputation_config = {
        "c": {
            "intermediate_column": "constructed",
            "marker": "C",
            "fill_column": "auxiliary_variable",
            # doesn't actually apply a fill so can be forward or back
            "fill_method": "ffill",
            "link_column": "construction_link",
        },
        "fir": {
            "intermediate_column": "fir",
            "marker": "FIR",
            "fill_column": "target",
            "fill_method": "ffill",
            "link_column": "cumulative_forward_imputation_link",
        },
        "bir": {
            "intermediate_column": "bir",
            "marker": "BIR",
            "fill_column": "target",
            "fill_method": "bfill",
            "link_column": "cumulative_backward_imputation_link",
        },
        "fic": {
            "intermediate_column": "fic",
            "marker": "FIC",
            # this has to have the same name as the intermediate column for constructed
            "fill_column": "constructed",
            "fill_method": "ffill",
            "link_column": "cumulative_forward_imputation_link",
        },
    }

    df.sort_values([imputation_class, reference, period], inplace=True)

    intermediate_columns = []

    for imp_type in imputation_config:
        df = create_impute(
            df, [imputation_class, reference], imputation_config[imp_type]
        )
        df = merge_imputation_type(
            df, imputation_config[imp_type], marker, combined_imputation
        )

        intermediate_columns.append(imputation_config[imp_type]["intermediate_column"])

    return df.drop(columns=intermediate_columns)


def create_impute(df, group, imputation_spec):
    """
    Add a new column to a dataframe of imputed values using ratio imputation.

    Parameters
    ----------
    dataframe : pandas.DataFrame
    group : str or list
        variables that define the imputation class
    imputation_spec: dict
        dictionary defining the details of the imputation type

    Returns
    -------
    pandas.DataFrame
        dataframe with an added imputation column defined by the imputation_spec
    """
    column_name = imputation_spec["intermediate_column"]
    fill_column = imputation_spec["fill_column"]
    fill_method = imputation_spec["fill_method"]
    link_column = imputation_spec["link_column"]

    df[column_name] = (
        df.groupby(group)[fill_column].fillna(method=fill_method) * df[link_column]
    )
    return df


def merge_imputation_type(df, imputation_spec, marker, combined_imputation):
    """
    Uses an existing column of imputed values and a imputation marker to merge values
    into a single column

    Parameters
    ----------
    dataframe : pandas.DataFrame
    imputation_spec: dict
        dictionary defining the details of the imputation type
    marker : str
        column name containing a marker to indicate the type of imputation required
    combined_imputation : str
        column name for the combined imputation types according to the imputation marker

    Returns
    -------
    pandas.DataFrame
        dataframe with combined_imputation
    """

    imputation_marker = imputation_spec["marker"]
    imputation_column = imputation_spec["intermediate_column"]

    df.loc[df[marker] == imputation_marker, combined_imputation] = df[imputation_column]
    return df


marker = "imputation_marker"
combined_imputation = "imputed_value"
reference = "reference"
period = "period"
imputation_class = "strata"
df_new = create_and_merge_imputation_values(
    df, imputation_class, reference, period, marker, combined_imputation
)
