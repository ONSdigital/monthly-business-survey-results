import pandas as pd


def pivot_imputation_value(
    df: pd.DataFrame,
    identifier: str,
    date: str,
    sic: str,
    cell: str,
    forward: str,
    backward: str,
    construction: str,
    question: str,
) -> pd.DataFrame:
  
    """
    Returning dataframe containing imputation_value, filtered by date, pivoted by imputation type
    and grouped by sic, cell, question and imputation type.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Reference dataframe with domain, a_weights, o_weights, and g_weights
    identifier : str
        name of column in dataframe containing identifier variable
    date : str
        name of column in dataframe containing period variable
    sic : str
        name of column in dataframe containing domain variable
    cell : str
        name of column in dataframe containing question code variable
    forward : str
        name of column in dataframe containing predicted value variable
    backward : str
        name of column in dataframe containing imputation marker variable
    construction : str
        name of column in dataframe containing a_weight variable
    question : str
        name of column in dataframe containing o_weight variable
    
    Returns
    -------
    dataframe filtered by date, containing imputation_value, pivoted by imputation type
    and grouped by sic, cell, question and imputation type.

    """
    
    df = df[df.date == 202001]

    df = df.drop_duplicates(subset=['date', 'sic', 'cell', 'question'])

    df = df.melt(id_vars=['date', 'sic', 'cell', 'question'],
                    value_vars=['forward', 'backward', 'construction'],
                    var_name='link_type',
                    value_name='imputation_link')

    link_type_map = {'forward': 'F', 'backward': 'B', 'construction': 'C'}
    df['link_type'] = df['link_type'].map(link_type_map)

    df['link_type'] = pd.Categorical(df['link_type'], categories=['F','B','C'], ordered=True)
    df.sort_values(['date', 'sic', 'cell', 'question', 'link_type'], inplace=True)

    return df.reset_index(drop=True)