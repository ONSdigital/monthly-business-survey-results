import pandas as pd

def calculate_construction_link(
        dataframe, target, auxiliary, match_flag, strata,
    ):
    """_summary_

    Parameters
    ----------
    dataframe : pandas.DataFrame
        _description_
    target : str
        name of column in dataframe containing target variable
    auxiliary : str
        name of column in dataframe containing auxiliary variable
    match_flag : str
        name of column in dataframe containing flag for valid construction matches
    strata : str
        name of column in dataframe containing strata variable
    """
    grouped_data = (
        dataframe[dataframe[match_flag] == True]
        .groupby(strata)
    )

    grouped_data["return_sum"] = grouped_data.sum(target)
    grouped_data["auxiliary_sum"] = grouped_data.sum(auxiliary)
    grouped_data["construction_link"] = grouped_data[["return_sum"]]/grouped_data["construction_link"]

    grouped_data = grouped_data[[strata, "construction_link"]]
    dataframe = dataframe.merge(grouped_data, how="left", on=strata)

    return dataframe
