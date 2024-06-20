import pandas as pd


def filter_responses(df, reference, period, last_update):
    """
    Filter the responses data to return only the most recently updated
    row for each reference and period combination

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing responses data from SPP

    reference : str
        Column name containing business reference id.
    period : str
        Column name for survey period.
    last_update : str
        Column name containing auxiliary data.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame of one reference period combination
    """
    return df.sort_values(last_update).groupby([reference, period]).tail(1)


def clean_and_merge(
    snapshot, reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Clean and merge JSON inputs.

    Parameters
    ----------
    snapshot: Dict
      raw input as loaded by research_and_development.utils.hdfs_load_json
    reference: Str
      the name of the reference column
    period: Str
      the name of the period column
    response_keep_cols: Str
      the names of the columns to keep from the responses data
    contributors_keep_cols: Str
        the names of the columns to keep from the contributors data
    **config: Dict
      main pipeline configuration. Can be used to input the entire config dictionary
    Returns
    -------
    pd.DataFrame
      Main analysis dataset

    """
    contributors = pd.DataFrame(snapshot["contributors"])
    responses = pd.DataFrame(snapshot["responses"])

    responses = filter_responses(responses, reference, period, "lastupdateddate")
    responses = responses[responses_keep_cols].set_index([reference, period])
    contributors = contributors[contributors_keep_cols].set_index([reference, period])

    return responses.join(contributors, on=[reference, period])


def enforce_datatypes(df, config):
    joint_dictionary = {
        **config["responses_keep_cols"],
        **config["contributors_keep_cols"],
    }
    for key in joint_dictionary:
        type_from_dict = joint_dictionary[key]
        print(type_from_dict)
        if type_from_dict in ["str", "float", "int"]:
            df[key] = df[key].astype(type_from_dict)
        elif type_from_dict == "DateTime":
            print("cv dt")
            df[key] = pd.to_datetime(df[key], format="%Y%m")
        else:
            print("check datatype for {}".format(key))
        return df
