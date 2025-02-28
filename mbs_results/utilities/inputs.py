import json
import os
from pathlib import Path
from typing import Dict, Any


def load_config_file(config_file: str = 'user_config.json') -> Dict[str, Any]:
    """Function loads a JSON file and returns its content as a dictionary.

    Args:
        config_file (str): Path to the JSON file.

    Returns:
        Dict[str, Any]: Parsed JSON content as a dictionary.
    """  
    with open(config_file, 'r') as file:
        data = json.load(file)
    return data


def construct_path(config_paths: Dict[str, str], file_paths: Dict[str, str], root_dir: str) -> str:
    """
    Constructs the full path for each file using the provided configuration.
    
    This function constructs the full path for each file by joining the root directory
    with the relative path for the file. It also ensures that the directories for those
    paths exist by calling the `ensure_directory_exists` function.
    
    Args:
        config (Dict[str, str]): A dictionary containing configuration keys and their
                                    corresponding file paths.
                                    
        file_paths (Dict[str, str]): A dictionary containing file keys and their relative paths.
        
        root_dir (str): The root directory where the parent directory will be created.
        
    Returns:
        Dict[str, str]: A dictionary with the same keys as the input, but with the
                        full file paths constructed by joining the base directory
                        with the relative paths
    """
    # Create the directory and subdirectory paths
    directory_paths = {}
    directory_paths['parent_path'] = Path(root_dir) / Path(config_paths.get("parent_path", None))
    directory_paths['input_path'] = directory_paths['parent_path'] / Path(config_paths.get("input_path", None))
    directory_paths['output_path'] = directory_paths['parent_path'] / Path(config_paths.get("output_path", None))
    directory_paths['mapping_path'] = directory_paths['parent_path'] / Path(config_paths.get("mapping_path", None))
    directory_paths['folder_path'] = directory_paths['parent_path'] / Path(config_paths.get("folder_path", None))
    
    #Check if the directory_paths exist and create them
    for key, value in directory_paths.items():
        ensure_directory_exists(value)
    
    # Mapping of the specific file paths to their corresponding config subdirectories
    path_mapping = {
        "population_path": "input_path",
        "sample_path": "input_path",
        "back_data_qv_path": "input_path",
        "back_data_cp_path": "mapping_path",
        "back_data_finalsel_path": "parent_path",
        "sic_domain_mapping_path": "mapping_path",
        "threshold_filepath": "mapping_path",
        "calibration_group_map_path": "mapping_path",
        "classification_values_path": "mapping_path",
        "l_values_path": "mapping_path",
        "manual_constructions_path": "parent_path",
        "mbs_file_name": "parent_path",
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
        config[key] = Path(relative_path) / Path(value)  # Using Path to join paths
        print("construct_path full_path:", config[key])
    return config


def ensure_directory_exists(directory_path: Path) -> None:
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
    #full_path = Path(full_path)
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
        file_paths (Dict[str, str]): A dictionary containing file keys and their relative paths.

    Returns:
        Dict[str, str]: A dictionary with the same keys as the input, but with the
                        full file paths constructed by joining the base directory
                        with the relative paths.
    """
    # Extract the 'config' and 'file_paths' dictionaries from the loaded JSON data
    config_paths = config_data['config_paths']
    file_paths = config_data['file_paths']
       
    try:
        # Get the parent directory based on the mapping
        root_dir = config_paths.get("s3_bucket", None)   
        # Check if root_dir is None or an empty string
        if root_dir not in (None, ""):
            root_dir = f"s3://{root_dir}"
            # Connect to s3 bucket in AWS cloud
        else:
            root_dir = config_paths.get("local_drive_directory", None)
            if root_dir is None:
                raise KeyError("Configuration does not contain 's3_bucket' or 'local_drive_directory' key.")
            # Concatenate the root directory with the parent directory

    except KeyError:
        raise KeyError("Configuration does not contain 's3_bucket' or 'local_drive_directory' key.")

    config = construct_path(config_paths, file_paths, root_dir)  # Construct the full path for each file

    return config


def main():
    # Load the config file
    config_file = Path('mbs_results/user_config.json')
    config_data = load_config_file(config_file=config_file)

    # Get all file paths and check directories
    config = get_file_paths(config_data)
        
    # Load the constants file
    config_constant_file = Path('mbs_results/config/constants.json')
    config_constant_data = load_config_file(config_file=config_constant_file)
    
    # Update the config dictionary with the constants
    config.update(config_constant_data)
   
    # Print all the dynamically constructed paths
    for key, path in config.items():
        print(f"key: {key} | value: {path}")


# Example of how to use these functions
if __name__ == "__main__":
    main()
