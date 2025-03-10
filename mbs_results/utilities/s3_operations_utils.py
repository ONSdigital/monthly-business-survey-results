import os
from io import BytesIO, StringIO
from typing import List, Optional, Union

import boto3
import pandas as pd
import raz_client
from botocore.exceptions import ClientError

from mbs_results import logger


def connect_to_s3() -> boto3.client:
    """
    Creates or returns an existing S3 client.

    This function checks if an S3 client has been provided. If not, it creates a new one using the `boto3.client`
    method. It also configures the S3 client with a custom SSL certificate and logs a success message once
    the connection is established.

    Parameters
    ----------
    None

    Returns
    -------
    boto3.client
        The S3 client that has been created or returned.

    Example
    -------
    # Use the function to create a new S3 client
    s3 = connect_to_s3()


    Notes
    -----
    - The client will be configured with a custom SSL certificate located at `/etc/pki/tls/certs/ca-bundle.crt`.
    - Logs a success message after successfully connecting to AWS S3.
    """
    s3_client = boto3.client("s3")
    raz_client.configure_ranger_raz(
        s3_client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
    )
    logger.info("Successfully connected to AWS S3.")
    return s3_client


def is_object_present(
    s3_client: boto3.client, bucket_name: str, s3_object_key: str
) -> bool:
    """Checks whether an object exists in the specified S3 bucket.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client object.
    bucket_name : str
        The name of the S3 bucket.
    s3_object_key : str
        The object key (path) to check for in the bucket.

    Returns
    -------
    bool : True if the object exists, False otherwise.

    Raises
    ------
    Exception
        If an error occurs during checking the object.
    """
    try:
        # List objects in the bucket with the specified prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_object_key)

        # Check if any objects match the prefix
        if "Contents" in response:
            return len(response["Contents"]) > 0
        else:
            return False

    except ClientError as e:
        # Log and raise the client-specific error if any issues with S3
        logger.error(f"ClientError occurred: {e}")
        raise e  # Re-raise the original exception

    except Exception as e:
        # Catch all other exceptions and raise a generic exception
        logger.error(f"An unexpected error occurred: {e}")
        raise Exception(
            f"Failed to check if the object exists in bucket {bucket_name}. Error: {str(e)}"
        )


def create_folder(s3_client: boto3.client, folder_name: str, bucket_name: str) -> None:
    """
    Creates a folder in the S3 bucket.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client object.
    folder_name : str
        The name of the folder to create.
    bucket_name : str
        The name of the S3 bucket.

    Returns
    -------
    None
        This function does not return any value.

    Raises
    ------
        If an error occurs during folder creation.
    """
    logger.info(f"Entered the create_folder function for folder: {folder_name}.")
    try:
        s3_client.Object(bucket_name, folder_name).load()
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            folder_obj = f"{folder_name}/"
            s3_client.put_object(Bucket=bucket_name, Key=folder_obj)
        logger.info(f"Exited the create_folder function for folder: {folder_name}.")


def create_folder_if_not_exists(
    s3_client: boto3.client, bucket_name: str, folder_name: str
) -> None:
    """
    Creates a folder in the S3 bucket if it does not exist.

    Parameters
    ----------
    s3_client : (boto3.client)
        The S3 client object.
    bucket_name : str
        The name of the S3 bucket.
    folder_name : str
        The folder (prefix) to create in the bucket.
    """
    try:
        # Check if the folder exists by listing objects with the given prefix (folder_name)
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_name, MaxKeys=1
        )

        # If 'Contents' is not in the response, the folder does not exist
        if "Contents" not in response:
            # Create an empty object in S3 to simulate a folder
            s3_client.put_object(Bucket=bucket_name, Key=folder_name + "/")
            logger.info(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")
        else:
            logger.info(
                f"Folder '{folder_name}' already exists in bucket '{bucket_name}'."
            )
    except ClientError as e:
        # Handle ClientError exceptions
        logger.error(f"ClientError occurred: {e}")
        raise e  # Re-raise the original exception
    except Exception as e:
        # Catch all other exceptions and raise a generic exception
        logger.error(f"An unexpected error occurred: {e}")
        raise Exception(
            f"Failed to create folder {folder_name} in bucket {bucket_name}. Error: {str(e)}"
        )


def create_s3_object(s3_client: boto3.client, bucket_name: str, s3_key: str) -> None:
    """
    Creates an S3 object in the specified bucket.
    This function uses the provided `s3_client` to create an S3 object in the specified
    `bucket_name` with the given `s3_key`.

    Check if the object already exists in the bucket. If it does not, the function
    creates an object as a placeholder.

    Parameters
    ----------
    s3_client : boto3.client
        The boto3 S3 client object.
    bucket_name : str
        The name of the S3 bucket where the object will be created.
    s3_key : str
        The key of the S3 object to be created.

    Returns
    -------
    None
        This function does not return any value.

    Raises
    ------
        If an error occurs during folder creation.
    """
    logger.info(f"Entered the create_s3_object function for object: {s3_key}.")
    try:
        # Attempt to head the object (check if it exists)
        s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        logger.info(f"File already exists: {s3_key}")
    except s3_client.exceptions.NoSuchKey:
        # Check for the 404 error code ('NoSuchKey')
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body="")
        logger.info(f"Created: {s3_key}")


def upload_dataframe_to_s3(
    bucket_name: str,
    s3_key: str,
    dataframe: pd.DataFrame,
    file_format: str = "csv",  # The file format for saving the DataFrame (csv, json, parquet)
    index: bool = False,  # Whether to include index in CSV/JSON
    compression: Union[
        str, None
    ] = None,  # Compression type for CSV/Parquet (e.g., 'gzip', 'snappy', or None)
) -> None:
    """
    Uploads a Pandas DataFrame to an S3 bucket in a specified file format.

    This function converts the provided DataFrame into a specified file format (CSV, JSON, or Parquet)
    and uploads it to an S3 bucket. It also supports compression for CSV and Parquet files.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket where the DataFrame will be uploaded.
    s3_key : str
        The key (path) within the S3 bucket to save the file.
    dataframe : pd.DataFrame
        The Pandas DataFrame to be uploaded.
    file_format : str, optional
        The file format to save the DataFrame in. Supported formats are 'csv', 'json', and 'parquet'.
        Defaults to 'csv'.
    index : bool, optional
        Whether to include the DataFrame index in the saved file. The default is False.
    compression : str or None, optional
        The compression format to use for the file. Supported values are 'gzip', 'snappy', or None.
        Defaults to None.

    Raises
    ------
    ValueError
        If the file format is invalid or the DataFrame cannot be converted to the specified format.
    ClientError
        If an error occurs while interacting with S3 (e.g., permissions issue, S3 service error).
    Exception
        If any other unexpected error occurs during the file conversion or upload process.

    Example
    -------
    # Upload a DataFrame to S3 in CSV format
    upload_dataframe_to_s3(
        bucket_name='my-bucket',
        s3_key='path/to/myfile.csv',
        dataframe=df,
        file_format='csv',
        index=False
    )

    # Upload a DataFrame to S3 in Parquet format with compression
    upload_dataframe_to_s3(
        bucket_name='my-bucket',
        s3_key='path/to/myfile.parquet',
        dataframe=df,
        file_format='parquet',
        compression='snappy'
    )

    Notes
    -----
    - The function supports 'csv', 'json', and 'parquet' formats.
    - For CSV and JSON formats, compression can be specified (e.g., 'gzip').
    - For Parquet, supported compression options are 'snappy', 'gzip', etc.
    - The function handles both converting the DataFrame into the appropriate format and uploading the file to S3.
    - In the event of an error, an appropriate exception is raised, and the error is logged.
    """
    s3_client = connect_to_s3()

    try:
        # Create an in-memory file-like object

        file_obj = None

        if file_format == "csv":
            # Convert DataFrame to CSV (in-memory)
            file_obj = StringIO()
            dataframe.to_csv(file_obj, index=index, compression=compression)
            file_obj.seek(0)
            # Convert StringIO to bytes
            file_obj_bytes = file_obj.getvalue().encode("utf-8")

        elif file_format == "json":
            # Convert DataFrame to JSON (in-memory)
            file_obj = StringIO()
            dataframe.to_json(file_obj, orient="records", lines=True)
            file_obj.seek(0)
            # Convert StringIO to bytes
            file_obj_bytes = file_obj.getvalue().encode("utf-8")

        elif file_format == "parquet":
            # Convert DataFrame to Parquet (in-memory)
            file_obj = BytesIO()
            dataframe.to_parquet(file_obj, compression=compression)
            file_obj.seek(0)
            file_obj_bytes = file_obj.getvalue()

        else:
            raise ValueError(
                "Invalid file format. Supported formats are 'csv', 'json', or 'parquet'."
            )

        # Check if the file_obj contains data
        if file_obj_bytes is None or len(file_obj_bytes) == 0:
            raise ValueError(
                f"Failed to convert DataFrame to {file_format} format. The file is empty."
            )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_obj_bytes,
            ContentType=f"application/{file_format}",
        )
        logger.info(f"DataFrame uploaded to S3 at '{s3_key}' as {file_format} format.")

    except ClientError as e:
        logger.error(f"ClientError occurred while uploading DataFrame to S3: {e}")
        raise e
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while uploading DataFrame to S3: {e}"
        )
        raise Exception(
            f"Failed to upload DataFrame to S3 at {s3_key}. Error: {str(e)}"
        )


def load_dataframe_from_s3(
    bucket_name: str,
    s3_key: str,
    file_format: str,
    compression: Union[str, None] = None,
) -> pd.DataFrame:
    """
    Reads a DataFrame from an S3 bucket.

    This function retrieves a file from an S3 bucket using the provided bucket name and S3 key, then reads the
    content into a pandas DataFrame. The file format is specified by the `file_format` argument, and optional
    compression is handled based on the file format.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket containing the file.
    s3_key : str
        The S3 key (path) of the file to be read.
    file_format : str
        The format of the file to be read. Supported formats are:
        - 'csv': Reads as CSV file.
        - 'json': Reads as JSON (lines-oriented).
        - 'parquet': Reads as a Parquet file.
    compression : str, optional
        The compression format of the file, default is None. Compression options depend on the file format:
        - For 'csv': 'gzip', 'bz2', 'zip', etc.
        - For 'parquet': 'gzip', 'snappy', etc.

    Returns
    -------
    pd.DataFrame
        The DataFrame read from the file in the specified format.

    Raises
    ------
    ValueError
        If the provided file format is invalid (i.e., not 'csv', 'json', or 'parquet').
    ClientError
        If an error occurs while retrieving the object from S3, such as incorrect permissions or file not found.
    Exception
        If an unexpected error occurs during the reading or conversion process.

    Example
    -------
    df = load_dataframe_from_s3(
        bucket_name='my-bucket',
        s3_key='path/to/file.csv',
        file_format='csv'
    )
    print(df.head())

    Notes
    -----
    - The function automatically handles different file formats (CSV, JSON, Parquet) and supports compression for
      those formats.
    - A `ClientError` is raised if the file cannot be accessed from the S3 bucket.
    - An exception is raised if the file format is unsupported or if an error occurs while reading or converting the file.
    """

    s3_client = connect_to_s3()

    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_obj = response["Body"].read()

        # Convert the file object to a DataFrame
        if file_format == "csv":
            file_obj_str = file_obj.decode("utf-8")
            return pd.read_csv(StringIO(file_obj_str), compression=compression)
        elif file_format == "json":
            file_obj_str = file_obj.decode("utf-8")
            return pd.read_json(StringIO(file_obj_str), lines=True)
        elif file_format == "parquet":
            return pd.read_parquet(BytesIO(file_obj))
        else:
            raise ValueError(
                "Invalid file format. Supported formats are 'csv', 'json', or 'parquet'."
            )

    except ClientError as e:
        logger.error(f"ClientError occurred while reading DataFrame from S3: {e}")
        raise e
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while reading DataFrame from S3: {e}"
        )
        raise Exception(
            f"Failed to read DataFrame from S3 at {s3_key}. Error: {str(e)}"
        )
