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

def create_imputation_link(df):

    mapping_dict = {
        'bir': 'b_link_adjustedresponse',
        'fir': 'f_link_adjustedresponse',
        'c': 'construction_link',
        'fic': 'f_link_adjustedresponse',
        'fimc': 'f_link_adjustedresponse',
        'mc': None
    }

    
    df = df[~((df['imputation_flags_adjustedresponse'] == "r") | df['imputation_flags_adjustedresponse'].isnull())].reset_index(drop = True)
    def get_imputation_link(row):
        flag = row['imputation_flags_adjustedresponse']
        column_name = mapping_dict.get(flag)
        if column_name:
            return row[column_name]
        return None

    df['imputation_link'] = df.apply(get_imputation_link, axis=1)

    return df
  
if __name__ == "__main__":
  input_df = pd.read_csv("monthly-business-survey-results/tests/data/outputs/pivot_imputation_value/create_imputation_link_input.csv", index_col=False)
  expected_output = pd.read_csv("monthly-business-survey-results/tests/data/outputs/pivot_imputation_value/create_imputation_link_output.csv", index_col=False)
  actual_output = create_imputation_link(input_df)
  
  expected_output
  actual_output
  
  expected_output.columns