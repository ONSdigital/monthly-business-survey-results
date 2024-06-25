import pandas as pd
from validation_checks import validate_indices


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

    validate_indices(responses, contributors)

    return responses.merge(contributors, on=[reference, period])
