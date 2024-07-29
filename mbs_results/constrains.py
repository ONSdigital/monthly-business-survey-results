import pandas as pd
import operator
import numpy as np
from typing import List


def replace_values_index_based(
        df: pd.DataFrame,
        target: str,
        a: int,
        compare: str,
        b: int
) -> None:
    """
    Perform comparisons between a subset of df with a and subset of df with b and
    replace target from a with target from b when comparison is met. Both a
    and b must exist in the first level index, the comparison is based on the 
    remaining indices.
    
    Note that this function does not return anything, it modifies the input 
    dataframe.
    
    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe to replace the values.
    target : str
        Column name for values to be replced.
    a : int
        Question_no to check.
    compare : str
        Logical operator to compare, accepted '>', '<','>=','<=','=='.
    b : int
        Question_no to check against.
    """
    # For improved perfomance
    df.sort_index(inplace=True)
    
    ops = {'>': operator.gt,
       '<': operator.lt,
       '>=': operator.ge,
       '<=': operator.le,
       '==': operator.eq}
    
    series_from_a = df.loc[a][target]
    
    series_from_b = df.loc[b][target]
    
    common_index = series_from_a.index.intersection(series_from_b.index)
    
    # Has format (period,reference) 
    index_to_replace = series_from_a[ops[compare](series_from_a[common_index] ,series_from_b[common_index])].index
    
    if len(index_to_replace)>0:
        for date_ref_idx in index_to_replace.values:
           
            # Has format (question,no,period,reference) 
            index_to_replace = (a,) + date_ref_idx
            index_to_replace_with = (b,) + date_ref_idx
            # Filter target based on the indices
            df.loc[index_to_replace,target] = df.loc[index_to_replace_with,target]
            df.loc[index_to_replace,"constrain_marker"]=f"{a} {compare} {b}"
