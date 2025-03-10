import os
import pandas as pd
from typing import Union, Dict
from mbs_results import logger
from mbs_results.utilities.s3_operations_utils import upload_dataframe_to_s3

def save_file(dataframe: pd.DataFrame,
              config: Dict[str, str],
              file_format: str, 
              file_path: str, 
              index: bool = False, 
              compression: Union[str, None] = None
              )-> None:
    
    """
    Saves a DataFrame to a specified location based on the platform configuration (either locally or to S3).

    This function checks the 'platform' in the `config` dictionary to determine whether to save the
    DataFrame to a local file system or to an S3 bucket. It then delegates the saving process to
    the respective function, either `save_dataframe_locally` or `upload_dataframe_to_s3`.
    
    Parameters
    ----------
    dataframe : pd.DataFrame
        The DataFrame to be saved.
    config : Dict[str, str]
        A dictionary containing configuration settings, including:
            - 'platform' : str
                Specifies the platform ('s3' for S3, 'network' for network or local storage).
            - 's3_bucket' : str
                The name of the S3 bucket (used only if platform is 's3').
    file_format : str
        The file format to save the DataFrame as (e.g., 'csv', 'parquet', 'json').
    file_path : str
        The path where the file should be saved. If 's3' is specified as the platform, this is the S3 key.
    index : bool, optional
        Whether to write the DataFrame's index to the file. Default is `False`.
    compression : Union[str, None], optional
        Compression format to use (e.g., 'gzip', 'zip'). Default is `None`.

    Raises:
        ValueError: If an unsupported 'platform' value is provided in the config (not 'network' or 's3').

    Example:
        save_file(dataframe=df,
                  config={'platform': 's3', 's3_bucket': 'my-bucket'},
                  file_format='csv',
                  file_path='/path/to/file.csv',
                  index=False,
                  compression='gzip')
    
    This will save the DataFrame locally as a gzipped CSV file.
    
    """
    
    if config['platform'] == 's3':
        upload_dataframe_to_s3(            
            bucket_name=config['s3_bucket'],
            s3_key=file_path,
            dataframe=dataframe,
            file_format=file_format,
            index=index,
            compression=compression
        )
    elif config['platform'] == 'network':
        save_dataframe_locally(
            dataframe=dataframe,
            file_path=file_path,
            file_format=file_format,
            index=index,
            compression=compression
        )
    else:
        raise ValueError(f"Unknown platform [{config['platform']}]. Only 's3' (S3) or 'network' are supported.")
    

def save_dataframe_locally(
    dataframe: pd.DataFrame, 
    file_path: str, 
    file_format: str, 
    index: bool = False, 
    compression: Union[str, None] = None
    ) -> None:
    
    """
    Saves a DataFrame to the local file system in the specified file format.

    This function saves a pandas DataFrame to a local file system based on the provided file path and format.
    It supports saving in 'csv', 'json', and 'parquet' formats. The function also handles optional compression
    and index saving for the 'csv' format.

    Parameters
    ----------
    dataframe : pd.DataFrame
        The DataFrame to be saved.
    file_path : str
        The path where the DataFrame should be saved.
    file_format : str
        The file format to save the DataFrame as. Supported formats are:
            - 'csv': Saves as CSV file.
            - 'json': Saves as JSON file (one record per line).
            - 'parquet': Saves as a Parquet file.
    index : bool, optional
        Whether to write the DataFrame's index to the file (only relevant for 'csv' format). Default is `False`.
    compression : Union[str, None], optional
        Compression format to use when saving the file. Supported options depend on the file format:
            - For 'csv': 'gzip', 'bz2', 'zip', etc.
            - For 'parquet': 'snappy', 'gzip', etc.
            Default is `None` (no compression).

    Raises:
        ValueError: If an unsupported file format is provided.
        Exception: If an error occurs during the file saving process.

    Example:
        save_dataframe_locally(
            dataframe=df,
            file_path='/path/to/file.csv',
            file_format='csv',
            index=False,
            compression='gzip'
        )

        This would save the DataFrame `df` as a gzipped CSV file at the specified location.

    Notes:
        - The function ensures that the target directory exists before attempting to save the file. If the directory does not exist, it will be created.
        - Only the following file formats are supported: 'csv', 'json', and 'parquet'. If a different format is specified, a `ValueError` will be raised.
    """
    
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if file_format == 'csv':
            dataframe.to_csv(file_path, index=index, compression=compression)
        elif file_format == 'json':
            dataframe.to_json(file_path, orient='records', lines=True)
        elif file_format == 'parquet':
            dataframe.to_parquet(file_path, compression=compression)
        else:
            raise ValueError(f"Invalid file format [{file_format}]. Supported formats are 'csv', 'json', or 'parquet'.")

        logger.info(f"DataFrame successfully saved locally at '{file_path}' as {file_format} format.")

    except Exception as e:
        logger.error(f"An error occurred while saving DataFrame locally: {e}")
        raise e