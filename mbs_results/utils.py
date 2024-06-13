import pandas as pd


def convert_column_to_datetime(dates):
    return pd.to_datetime(dates, yearfirst=True)
