"""
A class that initialises a single instance of boto3 client.
"""

import logging

import boto3
import raz_client

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class SingletonBoto:
    """Creates a global singleton instance of the boto3 client and a bucket."""

    _instance = None
    _bucket = None

    def __init__(self):
        raise RuntimeError("This is a Singleton, invoke get_client() instead.")

    @classmethod
    def get_client(cls, config={}):
        """Create a boto3 client if it does not exist.
        If it exists, returns the existing one.
        """
        if cls._instance is None:
            client = boto3.client("s3")
            raz_client.configure_ranger_raz(
                client, ssl_file=config["ssl_file"]
            )
            cls._bucket = config["bucket"]
            cls._instance = client
        return cls._instance

    @classmethod
    def get_bucket(cls):
        """
        Returns the s3 bucket name from the config so it can be used globally.
        """
        if cls._bucket is None:
            raise RuntimeError("Bucket is not set. Call get_client() first.")
        return cls._bucket
