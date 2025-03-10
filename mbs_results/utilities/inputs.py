import json
import os
from pathlib import Path
from typing import Any, Dict

from mbs_results import logger
from mbs_results.utilities.load_file_object import load_file
from mbs_results.utilities.s3_operations_utils import (
    connect_to_s3,
    create_folder_if_not_exists,
    create_s3_object,
    is_object_present,
    upload_dataframe_to_s3,
)
from mbs_results.utilities.save_file import save_file


def load_config_file(config_file: str) -> Dict[str, Any]:
    """
    Function loads a JSON file and returns its content as a dictionary.

    Paramters
    ----------
    config_file : str
        Path to the JSON file.

    Returns
    -------
    Dict[str, Any]
        Parsed JSON content as a dictionary.
    """
    with open(config_file, "r") as file:
        data = json.load(file)
    return data


def construct_path(config_data: Dict[str, str]) -> str:
    """
    Constructs the full path for each file using the provided configuration.

    This function constructs the full path for each file by joining the root directory
    with the relative path for the file. It also ensures that the directories for those
    paths exist by calling the `ensure_directory_exists` function.

    Paramters
    ----------
    config_data : Dict[str, str]
        A dictionary containing configuration keys and their corresponding file paths.

    Returns
    -------
    Dict[str, str]
        A dictionary with the same keys as the input, but with the full file paths
            constructed by joining the base directory with the relative paths
    """

    config_paths = config_data["config_paths"]
    file_paths = config_data["file_paths"]

    try:
        # Get the parent directory based on the mapping
        root_dir = config_paths.get("s3_bucket")
        # Check if root_dir is None or an empty string
        data_stored_in_s3 = False
        if config_paths.get("s3_bucket") not in (None, "") and config_paths.get(
            "platform"
        ) not in (
            None,
            "",
            "network",
        ):
            data_stored_in_s3 = True
            root_dir = ""
            # Connect to s3 bucket in AWS cloud
        else:
            root_dir = config_paths.get("local_drive_directory", None)
            if root_dir is None:
                logger.info(
                    "Configuration does not contain 's3_bucket' or "
                    "'local_drive_directory' key."
                )
                raise KeyError(
                    "Configuration does not contain 's3_bucket' or "
                    "'local_drive_directory' key."
                )

        logger.info(
            f"{config_paths.get('platform')} Platform and {root_dir} parent directory in used"
        )

    except KeyError:
        logger.info(
            "Configuration does not contain 's3_bucket' or "
            "'local_drive_directory' key."
        )
        raise KeyError(
            "Configuration does not contain 's3_bucket' or 'local_drive_directory' key."
        )

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
    if data_stored_in_s3:
        s3_client = connect_to_s3()
        s3_bucket = config_paths.get("s3_bucket")
        for key, value in directory_paths.items():
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
        relative_path = ""
        if relative_path_map is not None:
            relative_path = directory_paths.get(relative_path_map, None)

        # Use pathlib to join paths properly (handles different OS path conventions)
        config[key] = Path(relative_path) / Path(value)
        if data_stored_in_s3:
            # [optional] check if the file exist, else create an empty file as placeholder
            object_status = is_object_present(s3_client, s3_bucket, str(config[key]))
            logger.info(f"Is the object {str(config[key])} present? {object_status}")
            if not object_status:
                # Create the S3 object
                create_s3_object(s3_client, s3_bucket, str(config[key]))
        logger.info(f"construct_path full_path for Key {key}: {config[key]}")
    return config


def ensure_directory_exists(directory_path: Path) -> None:
    """
    Ensures that the specified directory exists. If the directory does not exist,
    it creates it.

    This function checks whether the directory at the given path exists. If it
    does not, the function creates the directory (including any necessary parent
    directories). If the directory already exists, it prints a message indicating that.

    Parameters
    ----------
    directory_path : str
    The path to the directory that needs to be checked and possibly created.

    Returns
    -------
    None
        This function does not return any value. It only log messages about
        the directory's status.
    """

    if os.path.isdir(directory_path):
        logger.info(f"Directory {directory_path} exists")
    else:
        os.makedirs(directory_path, exist_ok=True)
        logger.info(f"Doesn't exist. Created the new directory: {directory_path}")


def load_config():
    config_file = Path("mbs_results/config_user.json")
    config_data = load_config_file(config_file=config_file)

    # Get all file paths and check directories
    config = construct_path(config_data)

    config.update(config_data["user_data"])

    # Update config with key-value pairs from config_data where the key
    # doesn't already exisit in config
    for key, value in config_data["config_paths"].items():
        if key not in config:
            config[key] = value

    # Load the constants file
    config_dev_file = Path("mbs_results/config_dev.json")
    config_dev_data = load_config_file(config_file=config_dev_file)

    # Update the config dictionary with the constants
    config.update(config_dev_data)

    # Print all the dynamically constructed paths
    for key, path in config.items():
        logger.info(f"key: {key} | value: {path}")

    # Create a dummy DataFrame with 5 columns and 10 rows of personal information
    data = {
        "Name": [
            "Alice",
            "Bob",
            "Charlie",
            "David",
            "Eve",
            "Frank",
            "Grace",
            "Hannah",
            "Ivy",
            "Jack",
        ],
        "Age": [25, 30, 22, 35, 29, 40, 31, 28, 26, 33],
        "Gender": [
            "Female",
            "Male",
            "Male",
            "Male",
            "Female",
            "Male",
            "Female",
            "Female",
            "Female",
            "Male",
        ],
        "City": [
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
            "San Antonio",
            "San Diego",
            "Dallas",
            "Austin",
        ],
        "Occupation": [
            "Engineer",
            "Doctor",
            "Artist",
            "Teacher",
            "Nurse",
            "Developer",
            "Scientist",
            "Designer",
            "Manager",
            "Entrepreneur",
        ],
    }
    import pandas as pd

    df = pd.DataFrame(data)

    save_file(
        df,
        config,
        "csv",
        str(Path(config["output_path"]) / "dataframe_saved_to_csv.gzip"),
        compression="gzip",
    )
    save_file(
        df,
        config,
        "json",
        str(Path(config["output_path"]) / "dataframe_saved_to_json.json"),
    )
    save_file(
        df,
        config,
        "parquet",
        str(Path(config["output_path"]) / "dataframe_saved_to_parquet.parquet.gzip"),
        compression="gzip",
    )
    # save_file(df, config, 'pickle', str(Path(config['output_path']) / 'dataframe_saved_to_pickle.pickle'))

    logger.info(df.head())

    # load data from platform
    df_read = load_file(
        config,
        str(Path(config["output_path"]) / "dataframe_saved_to_csv.gzip"),
        "csv",
        compression="gzip",
    )
    logger.info(df_read)
    df_read = load_file(
        config,
        str(Path(config["output_path"]) / "dataframe_saved_to_json.json"),
        "json",
    )
    logger.info(df_read)
    df_read = load_file(
        config,
        str(Path(config["output_path"]) / "dataframe_saved_to_parquet.parquet.gzip"),
        "parquet",
    )
    logger.info(df_read)

    return config


# Example of how to use these functions
if __name__ == "__main__":
    config = load_config()
