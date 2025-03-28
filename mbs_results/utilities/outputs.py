import pandas as pd
import raz_client
import boto3
from rdsa_utils.cdp.helpers.s3_utils import write_csv


def write_csv_wrapper(
    df: pd.DataFrame,
    save_path: str,
    import_platform: str = "network",
    bucket_name: str = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Save a pandas dataframe as a CSV file in a path from S3 bucket or to a 
    network path.

    Parameters
    ----------
    data: pd.DataFrame
        The dataframe to write to the specified path.
    save_path : str
        The key (full path and filename) of the CSV file in the S3 bucket or
        in the network.
    bucket_name : str, optional
        The name of the S3 bucket,needed when `import_platform` is set to
         `s3`. The default is None.
    kwargs
        Additional keyword arguments to pass to the `pd.write_csv` method.

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
        
        write_csv(client,bucket_name, df, save_path,**kwargs)
        return True

    if import_platform == "network":
         
        df.to_csv(save_path,**kwargs)
        return True

    raise Exception("platform must either be 's3' or 'network'")