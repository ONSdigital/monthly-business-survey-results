import boto3
import pandas as pd
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import write_csv

from mbs_results.utilities.utils import get_versioned_filename


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

        write_csv(client, bucket_name, df, save_path, **kwargs)
        return True

    if import_platform == "network":

        df.to_csv(save_path, **kwargs)
        return True

    raise Exception("platform must either be 's3' or 'network'")


def save_df(df: pd.DataFrame, base_filename: str, config: dict, on_demand=True):
    """
    Adds a version tag to the filename and saves the dataframe based on
    settings in the config.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe to write to the specified path.
    base_filename : str
        The base text for the filename.
    config : str, optional
        The pipeline configuration
    on_demand: bool
        Wether to foce the save, default is True.

    Returns
    -------
    None
    """

    # export on demand
    if on_demand:

        filename = get_versioned_filename(base_filename, config)

        write_csv_wrapper(
            df,
            config["output_path"] + filename,
            config["platform"],
            config["bucket"],
            index=False,
        )
