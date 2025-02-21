import json
import os
from pathlib import Path
from typing import Dict, Any


def load_config_file(config_path: str = 'user_config.json') -> Dict[str, Any]:
    """Function loads a JSON file and returns its content as a dictionary.

    Args:
        config_path (str): Path to the JSON file.

    Returns:
        Dict[str, Any]: Parsed JSON content as a dictionary.
    """  
    with open(config_path, 'r') as file:
        data = json.load(file)
    return data


def construct_path(config: Dict[str, str], file_key: str, file_paths: Dict[str, str]) -> str:
    """
    Constructs the full path for a given file based on the provided config dictionary.

    This function checks the config to determine where to concatenate the file path.
    For files under specific subdirectories like `in/`, `out/`, and `mapping_files/`,
    the correct parent directory from the config is used.

    Args:
        config (Dict[str, str]): A dictionary containing configuration keys and their
                                 values, such as the 's3_bucket', 'parent_directory', and subdirectories.
                                 
        file_key (str): The key in the file_paths dictionary corresponding to the relative
                        file path.
                        
        file_paths (Dict[str, str]): The dictionary containing file paths as relative paths.

    Returns:
        str: The full file path constructed by joining the parent directory and the 
             relative path.
             
    Raises:
        KeyError: If the configuration dictionary does not contain the 's3_bucket' or
                  'parent_directory' key
    """
    # Mapping of the specific file paths to their corresponding config subdirectories
    path_mapping = {
        "population_path": "input_directory",
        "sample_path": "input_directory",
        "output_path": "output_directory",
        "back_data_qv_path": "input_directory",
        "back_data_cp_path": "mapping_files_directory",
        "back_data_finalsel_path": "parent_directory",
        "sic_domain_mapping_path": "mapping_files_directory",
        "threshold_filepath": "mapping_files_directory",
        "calibration_group_map_path": "mapping_files_directory",
        "classification_values_path": "mapping_files_directory",
        "l_values_path": "mapping_files_directory",
        "manual_constructions_path": "parent_directory",
        "mbs_file_name": "parent_directory",
    }
    
    try:
        # Get the parent directory based on the mapping
        root_dir = config.get("s3_bucket", None)  # Get the root directory
        if root_dir is not None:
            root_dir = f"s3://{root_dir}"
        else:
            root_dir = config["parent_directory"]        
        
        subdir_key = path_mapping.get(file_key, None)
        if subdir_key is None:
            parent_dir = config["parent_directory"]
        else:
            parent_dir = config["parent_directory"]
            parent_dir = Path(parent_dir) / subdir_key  # Get the parent directory for the key
    except KeyError:
        # raise KeyError(f"Configuration does not contain key '{file_key}' or corresponding subdirectory mapping.")
        pass

    # Get the relative path from the file_paths
    relative_path = file_paths[file_key]

    # Use pathlib to join paths properly (handles different OS path conventions)
    full_path = str(Path(root_dir) /     Path(parent_dir) / relative_path)  # Using Path to join paths
    print("construct_path full_path:", full_path)
    return str(full_path)


def ensure_directory_exists(directory_path: str) -> None:
    """
    Ensures that the specified directory exists. If the directory does not exist, it creates it.

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
    path = Path(directory_path)
    if not path.exists():
        print(f"Directory does not exist: {directory_path}. Creating now...")
        path.mkdir(parents=True, exist_ok=True)
    else:
        print(f"Directory already exists: {directory_path}")


def get_file_paths(config: Dict[str, str], file_paths: Dict[str, str]) -> Dict[str, str]:
    """
    Connects all the file paths using the provided configuration.

    This function iterates over the `file_paths` dictionary, constructs full paths
    for each file using the `construct_path` function, and ensures that the
    directories for those paths exist by calling the `ensure_directory_exists` function.

    Args:
        config (Dict[str, str]): A dictionary containing configuration keys and their
                                 corresponding file paths.
        file_paths (Dict[str, str]): A dictionary containing file keys and their relative paths.

    Returns:
        Dict[str, str]: A dictionary with the same keys as the input, but with the
                        full file paths constructed by joining the base directory
                        with the relative paths.
    """
    # Define the full file paths
    full_file_paths = {}

    for key, relative_path in file_paths.items():
        full_path = construct_path(config, key, file_paths)  # Construct the full path for each file
        full_file_paths[key] = full_path

        # Ensure all directories exist before proceeding
        directory = Path(full_path).parent  # Get the parent directory
        ensure_directory_exists(directory)

    return full_file_paths


# Example of how to use these functions
if __name__ == "__main__":
    # Load the config file
    config_file = 'user_config.json'
    config_data = load_config_file(config_path=config_file)

    # Extract the 'config' and 'file_paths' dictionaries from the loaded JSON data
    config = config_data['config']
    file_paths = config_data['file_paths']

    # Ensure the root directory exists (e.g., /release/)
    ensure_directory_exists(config['parent_directory'])

    # Get all file paths and check directories
    full_file_paths = get_file_paths(config, file_paths)

    # Print all the dynamically constructed paths
    for key, path in full_file_paths.items():
        print(f"Full path for {key}: {path}")
