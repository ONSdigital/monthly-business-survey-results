import pandas as pd

def load_and_format(filename):
    """Load csv as pandas dataframe and cast period column to datetime type"""
    df_loaded = pd.read_csv(filename)
    df_loaded['period'] = pd.to_datetime(df_loaded['period'], format='%Y%m')
    return df_loaded
