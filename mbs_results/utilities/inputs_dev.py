import json
import os
from pathlib import Path
from typing import Dict, Any
import boto3
import raz_client
from botocore.exceptions import ClientError
from mbs_results.utilities.s3_operations_utils import (
    connect_to_s3,
    is_object_present,
    create_folder_if_not_exists,
    create_s3_object,
    upload_dataframe_to_s3
)
from mbs_results import logger
from mbs_results.utilities.save_file import save_file
from mbs_results.utilities.load_file_object import load_file

def load_config_file(config_file: str = "user_config.json") -> Dict[str, Any]:
    """Function loads a JSON file and returns its content as a dictionary.

    Args:
        config_file (str): Path to the JSON file.

    Returns:
        Dict[str, Any]: Parsed JSON content as a dictionary.
    """
    with open(config_file, "r") as file:
        data = json.load(file)
    return data


def connect_to_s3_bucket(bucket_name: str) -> None:
    """Connects to an S3 bucket using the provided bucket name.

    This function uses the `boto3` library to connect to an S3 bucket in the AWS cloud.
    It uses the provided bucket name to establish the connection.

    Args:
        bucket_name (str): The name of the S3 bucket to which the connection will be
                           established.

    Returns:
        None: This function does not return any value.
    """
    # Connect to the S3 bucket
    s3_client = boto3.client("s3")
    raz_client.configure_ranger_raz(s3_client, ssl_file='/etc/pki/tls/certs/ca-bundle.crt')
    
    response = s3_client.list_objects(Bucket=bucket_name, Prefix="user/derrick.njobuenwu/")
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"Found file: {obj['Key']}")
    else:
        print("No objects found.")
    
    return s3_client


def create_s3_objec_1(s3_client, bucket_name, s3_key):
    """
    Creates an S3 object in the specified bucket.
    This function uses the provided `s3_client` to create an S3 object in the specified
    `bucket_name` with the given `s3_key`.
   
    Check if the object already exists in the bucket. If it does not, the function
    creates an empty object as a placeholder.

    Args:
        s3_client: The boto3 S3 client object.
        bucket_name: The name of the S3 bucket where the object will be created.
        s3_key: The key of the S3 object to be created.

    Returns:
        None: This function does not return any value.
    """

    # Check if object exists
    print(f"bucket_name: {bucket_name}, s3_key: {s3_key}")
    try:
        # Attempt to head the object (check if it exists)
        s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        print(f"File already exists: {s3_key}")
    except s3_client.exceptions.NoSuchKey:
        # Check for the 404 error code ('NoSuchKey')
            s3_client.put_object(Bucket=bucket_name, Key=f"{s3_key}/.nokeep", Body="")
            print(f"Created: {s3_key}")
            
        

def construct_path(
    config_paths: Dict[str, str],
    file_paths: Dict[str, str],
    root_dir: str,
    prod_run=False,
) -> str:
    """
    Constructs the full path for each file using the provided configuration.

    This function iterates over the `file_paths` dictionary, and constructs
    the full path for each file by joining the root directory with the
    relative path for the file. It also ensures that the directories for those
    paths exist by calling the `ensure_directory_exists` function if the platform is
    `local`. If the platform is not `local`, that is s3, it simply returns the
    constructed paths.

    Args:
        config_paths (Dict[str, str]): A dictionary containing configuration keys and
                                       their corresponding file paths.

        file_paths (Dict[str, str]): A dictionary containing file keys paths.

        root_dir (str): The root directory where the parent directory will be created
                        (s3 or local).
                       
        prod_run (bool): A boolean flag to indicate whether the code is running in
                            production mode (True) or not (False).

    Returns:
        Dict[str, str]: A dictionary with the same keys as the input, but with the
                        full file paths constructed by joining the base directory
                        with the relative paths
    """
    # Create the directory and subdirectory paths
    directory_paths = {}
    directory_paths["parent_path"] = Path(root_dir) / Path(
        config_paths.get("parent_path", None)
    )
    directory_paths["input_path"] = directory_paths["parent_path"] / Path(
        config_paths.get("input_path", None)
    )
    directory_paths["output_path"] = directory_paths["parent_path"] / Path(
        config_paths.get("output_path", None)
    )
    directory_paths["mapping_path"] = directory_paths["parent_path"] / Path(
        config_paths.get("mapping_path", None)
    )
    directory_paths["folder_path"] = directory_paths["parent_path"] / Path(
        config_paths.get("folder_path", None)
    )
    
    directory_paths["mbs_anonymised"] = directory_paths["parent_path"] / Path(
        config_paths.get("mbs_anonymised", None)
    )

    # Check if the directory_paths exist and create them
    if prod_run:
        # Connect to the S3 bucket
        s3_bucket = config_paths.get("s3_bucket")
        s3_client = connect_to_s3()
        
        for key, value in directory_paths.items():
            logger.info(f"key: {key} | value: {value}")
            create_folder_if_not_exists(s3_client, s3_bucket, str(value))
        
    else:
        for key, value in directory_paths.items():
            ensure_directory_exists(value)

    # Mapping of the specific file paths to their corresponding config subdirectories
    path_mapping = {
        "population_path": "input_path",
        "sample_path": "input_path",
        "back_data_qv_path": "input_path",
        "back_data_cp_path": "mbs_anonymised",
        "back_data_finalsel_path": "folder_path",
        "sic_domain_mapping_path": "mapping_path",
        "threshold_filepath": "mapping_path",
        "calibration_group_map_path": "mapping_path",
        "classification_values_path": "mapping_path",
        "l_values_path": "mapping_path",
        "manual_constructions_path": "folder_path",
        "mbs_file_name": "folder_path",
        "folder_path": "parent_path",
        "output_path": "parent_path",
    }
    config = {}
    for key, value in file_paths.items():
        relative_path_map = path_mapping.get(key, None)
        print(f"\nkey: {key} | relative_path_map: {relative_path_map}")
        relative_path = ""
        if relative_path_map is not None:
            relative_path = directory_paths.get(relative_path_map, None)

        print(f"relative_path: {relative_path}")

        # Use pathlib to join paths properly (handles different OS path conventions)
        config[key] = Path(relative_path) / Path(value)
        if prod_run:
            object_status = is_object_present(s3_client, s3_bucket, str(config[key]))
            logger.info(f"Is the object {str(config[key])} present? {object_status}") 
            if not object_status:     
                # Create the S3 object
                create_s3_object(s3_client, s3_bucket, str(config[key]))
        #print("construct_path full_path:", config[key])
    return config


def ensure_directory_exists(directory_path: Path) -> None:
    """
    Ensures that the specified directory exists. If the directory does not exist,
    it creates it.

    This function checks whether the directory at the given path exists. If it
    does not, the function creates the directory (including any necessary parent
    directories). If the directory already exists, it prints a message indicating that.

    Args:
        directory_path (str): The path to the directory that needs to be checked and
                              possibly created.

    Returns:
        None: This function does not return any value. It only prints messages about
              the directory's status.
    """
    # full_path = Path(full_path)
    print(f"directory_path: {directory_path}")

    if os.path.isdir(directory_path):
        print(f"Directory {directory_path} exists")
    else:
        print("Doesn't exists")
        os.mkdir(directory_path)
        print(f"Created parent directory: {directory_path}")


def get_file_paths(config_data: Dict[str, str]) -> Dict[str, str]:
    """
    Connects all the file paths using the provided configuration.

    This function iterates over the `file_paths` dictionary, constructs full paths
    for each file using the `construct_path` function, and ensures that the
    directories for those paths exist by calling the `ensure_directory_exists` function.

    Args:
        config (Dict[str, str]): A dictionary containing configuration keys and their
                                 corresponding file paths.
        file_paths (Dict[str, str]): A dictionary containing file keys and their
                                     relative paths.

    Returns:
        Dict[str, str]: A dictionary with the same keys as the input, but with the
                        full file paths constructed by joining the base directory
                        with the relative paths.
    """
    # Extract the 'config' and 'file_paths' dictionaries from the loaded JSON data
    config_paths = config_data["config_paths"]
    file_paths = config_data["file_paths"]

    try:
        # Get the parent directory based on the mapping
        root_dir = config_paths.get("s3_bucket")
        # Check if root_dir is None or an empty string
        prod_run = False
        if root_dir not in (None, "") and config_paths.get("platform") not in (
            None,
            "",
            "local",
        ):
            prod_run = True
            root_dir = ""
            # Connect to s3 bucket in AWS cloud
        else:
            root_dir = config_paths.get("local_drive_directory", None)
            if root_dir is None:
                raise KeyError(
                    "Configuration does not contain 's3_bucket' or "
                    "'local_drive_directory' key."
                )
            # Concatenate the root directory with the parent directory

    except KeyError:
        raise KeyError(
            "Configuration does not contain 's3_bucket' or 'local_drive_directory' key."
        )

    config = construct_path(
        config_paths, file_paths, root_dir, prod_run
    )  # Construct the full path for each file

    return config


def main():
    # Load the config file
    config_file = Path("mbs_results/user_config.json")
    config_data = load_config_file(config_file=config_file)

    # Get all file paths and check directories
    config = get_file_paths(config_data)

    # Load the constants file
    config_constant_file = Path("mbs_results/constants.json")
    config_constant_data = load_config_file(config_file=config_constant_file)

    # Update the config dictionary with the constants
    config.update(config_constant_data)
    config['platform'] = config_data['config_paths'].get("platform")
    config['s3_bucket'] = config_data['config_paths'].get("s3_bucket")
    
    
    # Create a dummy DataFrame with 5 columns and 10 rows of personal information
    data = {
        'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Hannah', 'Ivy', 'Jack'],
        'Age': [25, 30, 22, 35, 29, 40, 31, 28, 26, 33],
        'Gender': ['Female', 'Male', 'Male', 'Male', 'Female', 'Male', 'Female', 'Female', 'Female', 'Male'],
        'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin'],
        'Occupation': ['Engineer', 'Doctor', 'Artist', 'Teacher', 'Nurse', 'Developer', 'Scientist', 'Designer', 'Manager', 'Entrepreneur']
    }

    import pandas as pd
    df = pd.DataFrame(data)
    
    
    for key, path in config_data.items():
        print(f"key: {key} | value: {path}")
    
    # Save df to s3
    s3_bucket = config_data['config_paths'].get("s3_bucket")
    logger.info(f"s3_bucket: {s3_bucket}")
    s3_client = connect_to_s3()
    s3_key = Path(config['output_path']) / 'dataframe_saved_to_json.json'
    upload_dataframe_to_s3(s3_bucket, str(s3_key), df, file_format='json')
    
    
    save_file(df, config, 'csv', str(Path(config['output_path']) / 'dataframe_saved_to_csv.csv'))
    
    logger.info(df.head())
    
    # load data from platform
    df_read = load_file(config, str(Path(config['output_path']) / 'dataframe_saved_to_csv.csv'), 'csv')
    logger.info(df_read)


# Example of how to use these functions
if __name__ == "__main__":
    main()
