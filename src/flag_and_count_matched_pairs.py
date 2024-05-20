import pandas as pd
import numpy as np 

def flag_matched_pair(df,forward_or_backward,target, period, reference, strata, time_difference=1):
    """
    function to flag matched pairs using the shift method

    Parameters
    ----------
    df : pd.DataFrame
    
        pandas dataframe of original data
    shift : int
        number of rows to shift up or down
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)

    Returns
    -------
    _type_
        pandas dataframe with column added flagging forward matched pairs and 
        predictive target variable data column
    """    
    
    df = df.sort_values(by = [reference, period])

    if forward_or_backward == 'b':
        time_difference = -time_difference
        
    df[["predictive_"+target, "predictive_period"]] = df.groupby([reference, strata]).shift(time_difference)[[target, period]]
    df[forward_or_backward+"_match"] = df.groupby(["group","reference"]).shift(1)["return"].isnull().mul(df["return"].isnull())
    
    match_counts = df.groupby([strata, period])[forward_or_backward+"_match"].agg("sum").reset_index()
    
    return df, match_counts


