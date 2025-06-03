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


def get_finalsel_columns():
    """
    Returns the list of column names for finalsel data.
    """
    return [
        "ruref",
        "checkletter",
        "frosic2003",
        "rusic2003",
        "frosic2007",
        "rusic2007",
        "froempees",
        "employees",
        "froempment",
        "employment",
        "froFTEempt",
        "FTEempt",
        "frotover",
        "turnover",
        "entref",
        "wowentref",
        "vatref",
        "payeref",
        "crn",
        "live_lu",
        "live_vat",
        "live_paye",
        "legalstatus",
        "entrepmkr",
        "region",
        "birthdate",
        "entname1",
        "entname2",
        "entname3",
        "runame1",
        "runame2",
        "runame3",
        "ruaddr1",
        "ruaddr2",
        "ruaddr3",
        "ruaddr4",
        "ruaddr5",
        "rupostcode",
        "tradstyle1",
        "tradstyle2",
        "tradstyle3",
        "contact",
        "telephone",
        "fax",
        "seltype",
        "inclexcl",
        "cell_no",
        "formtype",
        "cso_tel",
        "currency",
    ]


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
def get_finalsel(finalsel_columns: list):
    """
    Reads the finalsel data file and returns a DataFrame with the specified columns.

    Parameters
    ----------
    finalsel_columns : list
        List of column names to include in the finalsel DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the finalsel data with the specified columns.

    """
    finalsel_filepath = (
        "C:/Users/njobud/Office for National Statistics/Legacy Uplift - MBS/"
        "mbs_anonymised_jan_2025_20250128T111044/finalsel009_202112"
    )

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


if __name__ == "__main__":

    scottish_outputs_columns = get_scottish_outputs_columns()
    print(f"\nscottish_outputs_column_name\n{scottish_outputs_columns}")
    print("=============================")

    winsorisation_filepath = (
        "d:/consultancy/mbs_artifacts/temp_outputs_new_env/"
        "winsorisation_output_0.0.2.csv"
    )
    df, column_name, difference_column_name, missing_columns = (
        analyse_winsorisation_output(winsorisation_filepath, scottish_outputs_columns)
    )
    print(f"\nwinsorisation_output_column_name\n{column_name}")

    print("\nDifference between column_name and scottish_outputs_columns:")
    print(difference_column_name)
    print(
        "\nElements in loaded_config['scottish_outputs'] that are not in column_name:"
    )
    print(missing_columns)
    print("=============================")

    finalsel_columns = get_finalsel_columns()
    finalsel_data = get_finalsel(finalsel_columns)
    print(f"\n\nfinalsel_data\n{finalsel_data}")
    print("=============================")

    df_scottish_outputs = scottish_outputs(df, scottish_outputs_columns, None)

    outputs_dir = "D:/consultancy/monthly-business-survey-results/testing_copying/"
