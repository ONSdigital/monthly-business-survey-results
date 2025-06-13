import pandas as pd

from mbs_results.utilities.inputs import read_colon_separated_file


def get_scottish_outputs_columns():
    """
    Returns the list of column names for Scottish outputs.
    """
    return [
        "period",
        "SUT",
        "cell",
        "RU",
        "name",
        "enterprise group",
        "SIC",
        "form type",
        "status",
        "%scottish",
        "frozen employment",
        "band",
        "response type",
        "error marker",
        "start date",
        "end date",
        "returned turnover",
        "adjusted turnover",
        "response type.1",
        "error marker.1",
        "returned to exports",
        "adjused to exports",
        "response type.2",
        "error marker.2",
        "returned total employment",
        "adjusted total employment",
        "response type.3",
        "error marker.3",
        "returned FTM",
        "adjusted FTM",
        "response type.4",
        "error marker.4",
        "returned PTM",
        "adjusted PTM",
        "response type.5",
        "error marker.5",
        "returned FTF",
        "adjusted FTF",
        "response type.6",
        "error marker.6",
        "returned PTF",
        "adjusted PTF",
        "response type.7",
        "error marker.7",
        "returned volume water",
        "adjusted volume water",
        "response type.8",
        "error marker.8",
    ]


def get_finalsel_columns(config):
    """
    Returns the list of column names for finalsel data.
    """
    return config.get("sample_column_names")


def analyse_winsorisation_output(
    winsorisation_filepath: str, scottish_outputs_columns: list
):
    """
    Reads the winsorisation output file and compares its columns with the Scottish
    outputs columns.

    Parameters
    ----------
    winsorisation_filepath : str
        Path to the winsorisation output file.
    scottish_outputs_columns : list
        List of expected Scottish outputs columns.

    Returns
    -------
    tuple
        A tuple containing:
        - List of columns in the winsorisation output file.
        - List of columns in the winsorisation output file but not in the Scottish
          outputs columns.
        - List of columns in the Scottish outputs columns but not in the winsorisation
          output file.
    """

    df = pd.read_csv(winsorisation_filepath)
    column_name = df.columns.to_list()

    # Compute the difference between the two lists
    difference_column_name = list(set(column_name) - set(scottish_outputs_columns))

    # Compute the elements in scottish_outputs_columns that are not in column_name
    missing_columns = list(set(scottish_outputs_columns) - set(column_name))

    return df, column_name, difference_column_name, missing_columns


# Get finalsel dataframe
def get_finalsel(config: dict, finalsel_columns: list):
    """
    Reads the finalsel data file and returns a DataFrame with the specified columns.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing the path to the finalsel data file.
    finalsel_columns : list
        List of column names to include in the finalsel DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the finalsel data with the specified columns.

    """
    finalsel_filepath = config.get("back_data_finalsel_path")

    finalsel_data = read_colon_separated_file(finalsel_filepath, finalsel_columns)

    return finalsel_data


def scottish_outputs(df: pd.DataFrame, scotish_columns: list, sup_data: pd.DataFrame):
    """
    Function to produce Scottish (and Welsh?) outputs
    Some data is not available from only MBS, do we need to request QBS data,
    is this for us to do or out of scope?

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    scotish_columns : list
        _description_
    sup_data : pd.DataFrame
        _description_
    """
    return df[scotish_columns]
