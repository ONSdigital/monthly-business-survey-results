import json

import boto3
import pandas as pd
import raz_client

from rdsa_utils.cdp.helpers.s3_utils import load_json


def get_dfs_from_spp(
    filepath: str,
    import_platform: str,
    bucket_name: str = None,
) -> (pd.DataFrame, pd.DataFrame):
    """
    Load in contributors and responses dataframes from SPP snapshot json, using either
    S3 buckets or data stored on network.

    Parameters
    ----------
    filepath : str
        Filepath of snapshot, either in S3 buckets or local filepath when importing from
        network
    import_platform : str
        Platform to import from. Must be either 's3' or 'network'
    bucket_name: str
        Name of bucket when importing from S3 buckets

    Returns
    -------
    (pd.DataFrame, pd.DataFrame)
        Contributors and responses dataframes from snapshot.

    """

    if import_platform == "s3":
        client = boto3.client("s3")
        raz_client.configure_ranger_raz(
            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
        )
        snapshot = load_json(client, bucket_name, filepath)
    elif import_platform == "network":
        with open(filepath, "r") as f:
            snapshot = json.load(f)
    else:
        raise Exception("platform must either be 's3' or 'network'")

    contributors = pd.DataFrame(snapshot["contributors"])
    responses = pd.DataFrame(snapshot["responses"])

    return contributors, responses
