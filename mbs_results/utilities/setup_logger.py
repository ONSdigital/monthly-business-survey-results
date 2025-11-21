import logging
import os

import boto3
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import upload_file


def setup_logger(logger_name: str, logger_file_path: str) -> logging.Logger:
    """
    Sets up a logger with the specified name and file path.
    Call this function once at the start of the pipeline.

    Parameters:
        logger_name (str): The name of the logger.
        logger_file_path (str): The file path where the log file will be stored. If
                                None, logs will only be printed to the console.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s: %(name)s: %(levelname)s: %(module)s: "
        "%(funcName)s: %(lineno)d] %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    if logger_file_path:
        file_handler = logging.FileHandler(logger_file_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def upload_logger_file_to_s3(config: dict, local_path: str) -> bool:
    """Uploads a local file/logger file to S3 using Raz for authentication.
    Parameters:
        config (dict): Configuration dictionary containing
                       - S3 Bucket
                       - Raz setting
                       - output_path
        local_path (str): Path to the local file to be uploaded.

    Returns:
        bool: True if upload is successful, False otherwise.
    """
    if config.get("platform") != "s3":
        return False
    client = boto3.client("s3")
    ssl_file = config.get("ssl_file", "/etc/pki/tls/certs/ca-bundle.crt")
    raz_client.configure_ranger_raz(client, ssl_file=ssl_file)
    bucket_name = config.get("bucket")
    object_name = os.path.join(config.get("output_path"), local_path)

    upload_status = upload_file(
        client, bucket_name, local_path, object_name, overwrite=True
    )

    return upload_status
