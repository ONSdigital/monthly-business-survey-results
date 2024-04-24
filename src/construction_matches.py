import pandas as pd

def find_construction_matches(dataframe, period):
    """
    Find matched records based on the contributor's auxiliary information for a
    given period of time

    Parameters
    ----------
    dataframe : pandas.DataFrame
        records
    period : string
        time in format YYYYMM
    """
    return dataframe