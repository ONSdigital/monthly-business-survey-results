from typing import List

import pandas as pd

from mbs_results.utilities.inputs import read_csv_wrapper

def convert_column_to_datetime(dates):
    """
    Convert string pandas series to datetime (from raw inputs).

    Parameters
    ----------
    dates : pd.Series.

    Returns
    -------
    df : pd.Series
    """
    return pd.to_datetime(dates, format="%Y%m")

def validate_colon_file_columns(
    filepath: str,
    column_names: List[str],
    import_platform: str = "network",
    client: boto3.client = None,
    bucket_name: str = None,
) -> pd.DataFrame:
    """Check if column_names match the columns in filepath.

    Parameters
    ----------
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket or 
        in the network.
    column_names : List[str]
        list of column names in data file.
    client : boto3.client, optional
         The boto3 S3 client instance, needed when `import_platform` is set to
         `s3`, he default is None.
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.
    kwargs
        Additional keyword arguments to pass to the `pd.read_csv` method.

    Raises
    ------
    Exception
       If length of columns is not alligned with the number of columns when 
       the dataframe is loaded.
    """

    df = read_csv_wrapper(
        filepath,import_platform,client,bucket_name,sep=":",names=column_names,nrows=1)
    
    if len(df.columns) is not len(column_names):
        raise Exception("Length of `column_names` is: ",
                        len(column_names),
                          "does not match the columns in file: ",
                          filepath)




def append_filter_out_questions(
    df: pd.DataFrame, filter_out_questions_path: str
) -> pd.DataFrame:
    """Appends data with question codes which were ommitted from the processing

    Parameters
    ----------
    df : pd.DataFrame
        Main dataframe to append.
    filter_out_questions_path : str
        File path with filtered out questions.

    Returns
    -------
    df : pd.DataFrame
       "Main dataframe with filtered-out questions. If the file is not found or empty,
        logs a FileNotFoundError error and returns original dataframe"

    """
    try:
        filter_out_questions_df = pd.read_csv(filter_out_questions_path)

        df = pd.concat([df, filter_out_questions_df])

    except FileNotFoundError:
        print(
            """File not found. Please check filter_out_questions_path path,
         filter_out_questions_df is being created by filter_out_questions()
         in mbs_results/staging/data_cleaning.py"""
        )

    return df
