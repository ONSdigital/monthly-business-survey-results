import pandas as pd

def shift_by_strata_period(
        df: pd.DataFrame,
        target: str,
        period: str,
        strata: str,
        reference: str,
        time_difference: int,
        new_col: str,
        **kwargs
) -> pd.DataFrame:
    """
    It will perform the usual shift by desired time_difference for each value
    in strata and for consecutive period.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of original data
    target : str
        Column name containing target variable to be shifted.
    period : str
        Column name containing time period.
    strata : str
        Column name containing strata information (sic).
    reference : str
        Column name containing business reference id.
    time_difference : int
        Number of periods to shift. Can be positive or negative.
    new_col : str
        Column name containing the shifted values.
     kwargs : mapping, optional
        A dictionary of keyword arguments passed into func.


    Returns
    -------
    df : pd.DataFrame
        Pandas dataframe of original data with a new column containing the
        shifted values.
    """
    
    df.sort_values([reference,strata, period], inplace=True)

    df[new_col] = (
      df.groupby((
             (
                 df[period] - pd.DateOffset(months=1)
                 != df.shift(1)[period]
             )
             | (df[strata].diff(1) != 0)
             | (df[reference].diff(1) != 0)
         )
         .cumsum())
      .shift(time_difference)[target])
    
    return df