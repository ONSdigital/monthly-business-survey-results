import pandas as pd
import numpy as np 

def flag_matched_pair(df, forward_or_backward, target, period, reference, strata, time_difference=1):
    """
    function to flag matched pairs using the shift method

    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    forward_or_backward : int
        number of rows to shift up or down
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)
    time_difference: int
        lookup distance for matched pairs

    Returns
    -------
    _type_
        two pandas dataframes: the main dataframe with column added flagging forward matched pairs and 
        predictive target variable data column and a second with QA information on the number of matches for each 
        time period, for each group.
    """    
    
    df = df.sort_values(by = [reference, period])

    if forward_or_backward == 'b':
        time_difference = -time_difference
        
    df[forward_or_backward+"_match"] = df.groupby([strata, reference]).shift(time_difference)[target].notnull().mul(df[target].notnull())
        
    return df
  
  
def count_matches(df, forward_or_backward, target, period, reference, strata):
    """
    function to flag matched pairs using the shift method

    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data with matched pair flags - see flag_matched_pair
    forward_or_backward : int
        number of rows to shift up or down
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)
    time_difference: int
        lookup distance for matched pairs

    Returns
    -------
    _type_
        two pandas dataframes: the main dataframe with column added flagging forward matched pairs and 
        predictive target variable data column and a second with QA information on the number of matches for each 
        time period, for each group.
    """    
    
    return df.groupby([strata, period])[forward_or_backward+"_match"].agg("sum").reset_index()
  
  
  