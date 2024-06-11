import numpy as np


def get_cumulative_links(
    dataframe,
    forward_or_backward,
    strata,
    reference,
    target,
    period,
    imputation_link,
    time_difference=1,
    **kwargs
):
    """
    Create cumulative imputation links for multiple consecutive periods
    without a return.

    Parameters
    ----------
    dataframe : pandas.DataFrame
    forward_or_backward: str
        either f or b for forward or backward method

    strata : str
        column name containing strata information (sic)
    reference : str
        column name containing business reference id
    target : str
        column name containing target variable
    period : str
        column name containing time period
    imputation_link : string
        column name containing imputation links
    time_difference : int
        time difference between predictive and target period in months
    kwargs : mapping, optional
        A dictionary of keyword arguments passed into func

    Returns
    -------
    pandas.DataFrame
        dataframe with imputation_group and
        cumulative_forward/backward_imputation_link column
    """

    dataframe.sort_values([strata, reference, period], inplace=True)
    dataframe["missing_value"] = np.where(dataframe[target].isnull(), True, False)

    dataframe["imputation_group"] = (
        (
            (
                dataframe["imputation_marker"]
                .ne(dataframe["imputation_marker"].shift().bfill())
                .astype(int)
                != 0
            )
            | (dataframe[strata].diff(time_difference) != 0)
            | (dataframe[reference].diff(time_difference) != 0)
        )
        .astype("int")
        .cumsum()
    )

    if forward_or_backward == "f":
        dataframe["cumulative_" + imputation_link] = dataframe.groupby(
            "imputation_group"
        )[imputation_link].cumprod()
    elif forward_or_backward == "b":
        dataframe["cumulative_" + imputation_link] = (
            dataframe[::-1].groupby("imputation_group")[imputation_link].cumprod()[::-1]
        )

    dataframe["cumulative_" + imputation_link] = np.where(
        ~dataframe[target].isnull(),
        np.nan,
        dataframe["cumulative_" + imputation_link],
    )

    return dataframe
