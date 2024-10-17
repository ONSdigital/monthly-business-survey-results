import pandas as pd


def mapping_validation(
    df: pd.DataFrame,
    mapping_path: str,
    df_column_name: list,
    mapping_file_column_name: list,
):

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
        unmatched = list(set(unmatched))
        mapping_file_name = mapping_path.split("/")[-1]
        raise Warning(
            f"The following values from {df_column_name} in input dataframe "
            + f"are not mapped using {mapping_file_name}: \n {unmatched}"
        )
