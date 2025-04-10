import pandas as pd

from mbs_results.utilities.inputs import load_config


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
    pd.DataFrame
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


def create_imputation_link_output(
    additional_outputs_df: pd.DataFrame, sic,**config
) -> pd.DataFrame:
    """
    A wrapper function that runs the necessary functions for creating the
    imputation_link output.

    Parameters
    ----------
    addional_outputs_df : pd.DataFrame
        Dataframe containing the variables used for creating additional outputs.


    Returns
    -------
    pd.DataFrame
        Dataframe formatted according to the imputation_link output requirements.
    """
    output_df = (
        additional_outputs_df.pipe(create_imputation_link_column)
        .pipe(create_count_imps_column)
        .pipe(format_imputation_link,sic=sic)
    )

    return output_df


def create_imputation_link_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates an 'imputation_link' column based on specified rules for imputation flags.

    This function:
    1. Filters out rows where 'imputation_flags_adjustedresponse' is either 'r' or null.
    2. Creates an 'imputation_link' column by mapping the value of
    'imputation_flags_adjustedresponse' to the appropriate link column using the
    variable mapping_dict.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the following required columns:
        - 'imputation_flags_adjustedresponse': Specifies the type of link to use
            for imputation.
        - 'f_link_adjustedresponse': Forward link values for imputation.
        - 'b_link_adjustedresponse': Backward link values for imputation.
        - 'construction_link': Construction link values for imputation.


    Returns
    -------
    pd.DataFrame
        Dataframe containing an imputation_link column which takes values from the
        relevant link columns as specified in the mapping_dict variable.
    """

    mapping_dict = {
        "bir": "b_link_adjustedresponse",
        "fir": "f_link_adjustedresponse",
        "c": "construction_link",
        "fic": "f_link_adjustedresponse",
        "fimc": "f_link_adjustedresponse",
        "mc": None,
    }

    df = df[
        ~(
            (df["imputation_flags_adjustedresponse"] == "r")
            | df["imputation_flags_adjustedresponse"].isnull()
        )
    ].reset_index(drop=True)

    def get_imputation_link(row):
        flag = row["imputation_flags_adjustedresponse"]
        column_name = mapping_dict.get(flag)
        if column_name:
            return row[column_name]
        return None

    df["imputation_link"] = df.apply(get_imputation_link, axis=1)

    return df


def create_count_imps_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'count_imps' column to the input DataFrame based on the size of
    groups formed by the 'cell_no' column.

    This function groups the input DataFrame by the 'cell_no' column and calculates
    the size of each group. It then assigns the size value to all rows within the
    respective group as a new column named 'count_imps'.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing cell_no and any other relevant variables.

    Returns
    -------
    pd.DataFrame
        Dataframe containing the columns in the original dataframe plus a count_imps
        column.
    """
    df["count_imps"] = df.groupby("cell_no")["cell_no"].transform("size")
    return df


def format_imputation_link(df: pd.DataFrame, sic:str) -> pd.DataFrame:
    """
    Selects the relevant columns and renames them to match the expected imputation_link
    output format.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing currently selected sic column, cell_no, questioncode, imputation_link, # noqa: E501
        imputation_flags_adjustedresponse and count_imps.

    Returns
    -------
    pd.DataFrame
        Dataframe formatted according to the requirements for the imputation_link
        output.
    """
    
    df = df[
        [
            sic,
            "cell_no",
            "questioncode",
            "imputation_link",
            "imputation_flags_adjustedresponse",
            "count_imps",
            "adjustedresponse",
        ]
    ]

    renamed_df = df.rename(
        columns={
            sic: "sic",
            "cell_no": "cell",
            "questioncode": "Question",
            "imputation_flags_adjustedresponse": "link_type",
            "count_imps": "Count_imps",
            "adjustedresponse": "Imputation_value",
        }
    )

    return renamed_df
