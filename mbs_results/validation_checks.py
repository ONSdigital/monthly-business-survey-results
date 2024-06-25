def validate_config(config):
    """
    Run validation checks on the main config dictionary

    Parameters
    ----------
    config: Dict
      main pipeline configuration
    Returns
    -------
    pd.DataFrame
      Main analysis dataset

    """

    if colnames_clash(**config):
        raise ValueError(
            """Overlapping column names in responses_keep_cols and and
            contributors_keep_cols (main config)."""
        )


def colnames_clash(
    reference, period, responses_keep_cols, contributors_keep_cols, **config
):
    """
    Check for overlapping columns between the input data to be kept,
    except for id variables

    Parameters
    ----------
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

    return any(
        [
            i in contributors_keep_cols and i not in [reference, period]
            for i in responses_keep_cols
        ]
    )


def validate_indices(responses, contributors):
    """
    Check that all indices (reference x period) match
    across the responses and contributors dataframes.

    Raises an error if there are any mismatches.

    Parameters
    ----------
    responses: pd.DataFrame
      the responses dataset with index variables set
    contributors: pd.DataFrame
      the contributors dataset with index variables set

    """

    diff = set(responses.index.unique()) ^ set(contributors.index.unique())

    if len(diff) > 0:
        non_matches = "".join([f"reference: {i[0]}, period: {i[1]}\n" for i in diff])
        raise ValueError(
            f"""There are mismatched records between the responses and
            contributors datasets:\n {non_matches}"""
        )
