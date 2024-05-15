import pandas as pd


def get_cumulative_links(dataframe, forward_or_backward, time_difference, strata, reference, target, period, imputation_link):
    """
    Create cumulative imputation links for multiple consecutive periods without a return.

    Parameters
    ----------
    dataframe : pandas.DataFrame
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
    imputation_link : string
        column name containing imputation links

    Returns
    -------
    pandas.DataFrame
        dataframe with additional missing_value, imputation group and 
        cumulative_imputation_link column
    """

    if forward_or_backward == "f":
        pass
    elif forward_or_backward == "b":
        time_difference = -time_difference
        
    dataframe.sort_values([strata, reference, period], inplace=True)
    dataframe["missing_value"] = np.where(df[target].isnull(), True, False)
    
    dataframe["imputation_group"] = (
        (dataframe["missing_value"].diff(time_difference) != 0)
    .astype("int")
    .cumsum()
    )
    
    if forward_or_backward == "f":
        dataframe["cumulative_"+imputation_link] = (
            dataframe.groupby("imputation_group")[imputation_link]
            .cumprod()
        )
    elif forward_or_backward == "b":
        dataframe["cumulative_"+imputation_link] = (
            dataframe.groupby("imputation_group")[imputation_link][::-1]
            .cumprod()[::-1]
        )        
    
    return dataframe