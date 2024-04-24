import pandas as pd
import numpy as np 


#df['id','stratum','question']
# Col names as parameters - pass as list
def flag_matched_pair_merge(df,delta_t):#,ref_col,period_col,target_col,stratum_col):
    """
    function to add flag to df if data forms a matched pair
    i.e. data is given for both period and predictive period
    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    delta_t : int
        time difference between predictive and target period
         Should this be a more meaningful name? can work forwards or backwards

    Returns
    -------
    pd.DataFrame
        dataframe with column added added for flagging forward matched paris
    """    


    df_with_predictive_column = df[["reference", "stratum", "target_variable"]]
    df_with_predictive_column["predictive_period"] = df["period"] + delta_t
    df_with_predictive_column.rename(columns={'target_variable' : 'predictive_target_variable'},inplace = True)
    # Convert to datetime or write own implementation 
    
    df = df.merge(df_with_predictive_column,
                  left_on=["reference", "period", "stratum"],
                  right_on=["reference", "predictive_period", "stratum"],
                  how="left")
        
    matched_col_name = str(np.where(delta_t > 0,'f_matched_pair','b_matched_pair'))
    # should forwards be with positive delta_t or minus?

    df[matched_col_name] = np.where(
        df[['target_variable','predictive_target_variable']].isnull().any(axis=1),
        False, 
        True)
    return df
 

def count_matched_pair(df,matched_col_name):
    """
    function to count the number of forward matched pair per period
    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data

    Returns
    -------
    pd.DataFrame
        dataframe with column added for count of forward matched pairs
    """    
    df["match_pair_count"] = df.groupby(["stratum", "period"])[matched_col_name].transform("sum")
    return df

