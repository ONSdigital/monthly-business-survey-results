import pandas as pd
import numpy as np 

def flag_matched_pair_merge(df, forward_or_backward,target, period, reference, strata, time_difference=1):
    """
    function to add flag to df if data forms a matched pair
    i.e. data is given for both period and predictive period
    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    forward_or_backward: str
        either f or b for forward or backward method
    target : str
        column name containing target variable
    period : str
        column name containing time period
    reference : str
        column name containing business reference id
    strata : str
        column name containing strata information (sic)
    time_difference : int
        time difference between predictive and target period in months


    Returns
    -------
    pd.DataFrame
        dataframe with column added flagging forward matched paris and 
        predictive target variable data column
    """    

    if forward_or_backward == 'f':
        time_difference = time_difference
    elif forward_or_backward == 'b':
        time_difference =  -time_difference

    # Creating new DF, shifting period for forward or backward
    df_with_predictive_column = df[[reference, strata, target]]
    df_with_predictive_column["predictive_period"] = df[period] + pd.DateOffset(months=time_difference) 
    df_with_predictive_column.rename(columns={target : 'predictive_'+target},inplace = True)

    
    df = df.merge(df_with_predictive_column,
                  left_on=[reference, period, strata],
                  right_on=[reference, "predictive_period", strata],
                  how="left")

    matched_col_name = forward_or_backward + '_matched_pair'

    df[matched_col_name] = np.where(
        df[[target,'predictive_'+target]].isnull().any(axis=1),
        False, 
        True)
    
    df.drop(['predictive_period'],axis = 1, inplace=True)
    return df


def flag_matched_pair_shift(df,forward_or_backward,target, period, reference, strata, shift=1):
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
    
    if forward_or_backward == 'f':
        shift = shift
    elif forward_or_backward == 'b':
        shift = -shift

    df.sort_values([reference,period], inplace=True)
    df[["predictive_"+target, "predictive_period"]] = df.groupby([reference, strata]).shift(shift)[[target, period]]

    df["validate_date"] = np.where(df[period].dt.month - df["predictive_period"].dt.month == shift, True, False)
    matched_col_name = forward_or_backward + '_matched_pair'

    df[matched_col_name] = np.where(
    df[[target,'predictive_target_variable']].isnull().any(axis=1) | (df["validate_date"] != True),
    False, 
    True)

    df.drop(['validate_date','predictive_period'],axis = 1, inplace=True)

    return df
 

def count_matched_pair(df,matched_col_name):
    """
    function to count the number of forward matched pair per period
    Parameters
    ----------
    df : pd.DataFrame
        pandas dataframe of original data
    
    matched_col_name : str
        name of column containing flags if a matched pair is formed

    Returns
    -------
    pd.DataFrame
        dataframe with column added for count of forward matched pairs
    """   
    count_col_name = matched_col_name.split('_')[0]+'_matched_pair_count'
    df[count_col_name] = df.groupby(["stratum", "period"])[matched_col_name].transform("sum")
    return df

