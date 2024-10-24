import pandas as pd


def convert_ni_uk(dataframe: pd.DataFrame, cell_number: str):
    dataframe[cell_number] = (
        dataframe[cell_number]
        .astype(str)
        .map(lambda x: str(5) + x[1:] if x[0] == str(7) else x)
        .astype(int)
    )
    return dataframe
