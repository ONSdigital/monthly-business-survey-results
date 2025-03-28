import os
from typing import List

import mbs_results
from mbs_results import logger
from mbs_results.utilities.merge_two_config_files import merge_two_config_files
from rdsa_utils.cdp.helpers.s3_utils import load_csv
import boto3
import raz_client
import pandas as pd
def load_config(config_user_dict=None):
    # Get the directory where mbs_results is installed
    parent_dir = os.path.dirname(mbs_results.__file__)

    # Get config paths
    config_user_path = "config_user.json"
    config_dev_path = os.path.join(parent_dir, "configs", "config_dev.json")

    config = merge_two_config_files(config_user_path, config_dev_path)
    logger.info(
        f"config dictionary created from merging {config_user_path} "
        f"and {config_dev_path}"
    )

    if config_user_dict is not None:
        config.update(config_user_dict)
        logger.info(
            "config dictionary updated with config user dictionary from the testing"
        )
    return config

def read_csv_wrapper(
    filepath: str,
    import_platform: str = "network",
    bucket_name: str = None,
    client: boto3.client = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Load a CSV file from an S3 bucket or from a network path into a Pandas 
    DataFrame.

    Parameters
    ----------
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket or 
        in the network.
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.
    client : boto3.client, optional
         The boto3 S3 client instance, needed when `import_platform` is set to
         `s3`, the default is None.
    kwargs
        Additional keyword arguments to pass to the `pd.read_csv` method.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing the data from the CSV file.

    Raises
    ------
    Exception
        If import_platform is not either be 's3' or 'network'.
    """
    if import_platform == "s3":
        client = boto3.client("s3")
        raz_client.configure_ranger_raz(
            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
        )
        df = load_csv(
            client = client,
            bucket_name = bucket_name,
            filepath = filepath,
            **kwargs)
        
        return df

    if import_platform == "network":
        df = pd.read_csv(filepath, **kwargs)
        return df 
    
    raise Exception("platform must either be 's3' or 'network'")
    

def read_colon_separated_file(
    filepath: str,
    column_names: List[str],
    keep_columns: List[str] = None,
    period="period",
    import_platform: str = "network",
    client: boto3.client = None,
    bucket_name: str = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Load a CSV file from an S3 bucket or from a network path into a Pandas 
    DataFrame.

    Parameters
    ----------
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket or 
        in the network.
    client : boto3.client, optional
         The boto3 S3 client instance, needed when `import_platform` is set to
         `s3`, he default is None.
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.
    kwargs
        Additional keyword arguments to pass to the `pd.read_csv` method.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing the data from a colon seperated file.
    """
    usecols = None #pd.read_csv default load all columns

    if keep_columns:

        # position of columns to keep
        usecols = [column_names.index(x) for x in keep_columns]

        # pd.reader ingores order, usecols=[2,0,1] is same as [0,1,2]
        # ordered column names (myst align with usecols)
        column_names = [x for _, x in sorted(zip(usecols, keep_columns))]

    validate_colon_file_columns(filepath,column_names,import_platform,client,bucket_name)
    
    df = read_csv_wrapper(
        filepath,import_platform,client,bucket_name,sep=":",names=column_names,usecols=usecols)
    
    date_string = re.findall(r"_(\d{6})", filepath)

    #Get pattern from end, to avoid issues when path has dates 
    #e.g. path_190812/file_202301 should return 202301

    df[period] = int(date_string[-1])

    return df