import warnings

import pandas as pd


def wrap_mapping_validations(df: pd.DataFrame, mapping_folder: str):
    """
    wrapper to loop over the specified mapping files in file_and_column_names dict.
    Calls mapping_validation for the specified files
    Probably could move the files and column names to the config if needed.

    Parameters
    ----------
    df : pd.DataFrame
        input data to test against mapping files
    mapping_folder : str
        folder where the mapping files can be located.
        String must end with a '/' as we need this to be a folder where other mapping
        files are located
    """
    # Might be best to refactor this down the line and put into config if used
    files_and_column_names = {
        "sic_sut_mapping.csv": {
            "df_column_name": "sic_5_digit",
            "mapping_file_col_name": "sic",
        },
        "classification_sic_mapping.csv": {
            "df_column_name": "sic_5_digit",
            "mapping_file_col_name": "sic_5_digit",
        },
        "sic_domain_mapping.csv": {
            "df_column_name": "sic_5_digit",
            "mapping_file_col_name": "sic_5_digit",
        },
    }

    for i in files_and_column_names:
        mapping_path = mapping_folder + i
        column_dict = files_and_column_names[i]
        mapping_validation(
            df,
            mapping_path,
            column_dict["df_column_name"],
            column_dict["mapping_file_col_name"],
        )


def mapping_validation(
    df: pd.DataFrame,
    mapping_path: str,
    df_column_name: str,
    mapping_file_column_name: str,
):
    """
    validation function to check mapping file against a column within a given dataframe
    Only works with 'true' mapping files which two columns. Mapping files containing
    three columns will produce an error.

    Parameters
    ----------
    df : pd.DataFrame
        input dataframe containing the data used in pipeline
    mapping_path : str
        path to mapping file to validate
    df_column_name : str
        column name of column to join on in df
    mapping_file_column_name : str
        column name of column to join on from mapping file

    Raises
    ------
    Warning
        Warns if data within the original dataframe has not been mapped using
        the mapping file supplied
    """

    mapping_df = pd.read_csv(mapping_path)
    df_subset = df[df_column_name]
    new_column_name = [
        x for x in mapping_df.columns if x not in mapping_file_column_name
    ]
    new_column_name = "".join(new_column_name)
    df_subset = pd.merge(
        left=df_subset,
        right=mapping_df,
        left_on=df_column_name,
        right_on=mapping_file_column_name,
        how="left",
    )
    unmatched = df_subset.loc[
        df_subset[new_column_name].isna(), df_column_name
    ].to_list()

    if unmatched:
        unmatched = set(unmatched)
        mapping_file_name = mapping_path.split("/")[-1]
        warnings.warn(
            f"\n \n The following values from {df_column_name} in input dataframe "
            + f"are not mapped using {mapping_file_name}: \n {unmatched}"
        )
