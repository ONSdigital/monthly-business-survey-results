"""This is a stand alone pipeline to selectively transfer output files from
the output folder to the outgoing folder, along with their manifest file."""

import getpass
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List

import tomli

import mbs_results.utilities.merge_two_config_files as utils
from mbs_results.utilities.manifest_output import Manifest

# from src.utils.config import config_setup

# Set up logging

OutgoingLogger = logging.getLogger(__name__)

# Setting logging levels
warning_only = ["raz_client_logger", "requests_kerberos.kerberos_", "spnego._gss"]
for module in warning_only:
    logging.getLogger(module).setLevel(logging.WARNING)

# Set up default config
config_default = {
    "platform": "s3",
    "bucket": "onscdp-dev-data01-5320d6ca",
    "ssl_file": "/etc/pki/tls/certs/ca-bundle.crt",
}


def get_schema_headers(config: dict, file_select_dict: dict):
    """
    Extracts the schema headers from the provided configuration.

    This function filters the configuration for entries containing "schema" in
    their output name, and loads the corresponding TOML files. It then converts
    the headers (keys of the dict) into a comma-separated string and returns a
    dictionary where the keys are output names and the values are these
    comma-separated strings of schema headers.

    Args:
        config (dict, optional): A dictionary containing configuration details.
        Defaults to config.

    Returns:
        dict: A dictionary where the keys are export types and the values are
        comma-separated strings of schema headers.
    """
    schemas_dir = config["general"]["schemas_dir"]
    export_types = list(file_select_dict.keys())

    schema_paths = {type: schemas_dir + type + "_schema.toml" for type in export_types}

    # Get the headers for each
    schema_headers_dict = {}
    for output_name, path in schema_paths.items():
        with open(path, "rb") as file:
            schema_headers_dict[output_name] = tomli.load(file)

    # Stringify the headers (keys of the dict)
    schema_headers_dict.update(
        {
            output_name: ",".join(keys)
            for output_name, keys in schema_headers_dict.items()
        }
    )

    return schema_headers_dict


# Create a datetime object for the pipeline run - TODO: replace this with
# the pipeline run datetime from the runlog object
pipeline_run_datetime = datetime.now()


def get_file_choice(paths, config: dict):
    """
    Constructs a dictionary of file paths based on user's choices from the
    configuration.

    This function extracts the user's choices from the configuration,
    specifically entries where the key contains "output".
    It then constructs a dictionary where the keys are directory names
    (with the "output" prefix removed) and the values are
    complete file paths with a ".csv" extension. The file paths are constructed
    by joining the root output path, directory name,
    and file name from the configuration.

    Args:
        paths (dict): A dictionary containing various paths. The function
        specifically uses the "output_path" key to get the root output path.
        config (dict, optional): A dictionary containing configuration details,
        including the user's choices for files to export. Defaults to config.

    Returns:
        dict: A dictionary where the keys are directory names and the values are
        complete file paths to the files to be exported.

    Logs:
        The function logs the list of files being exported at the INFO level.
    """
    # Get the user's choices from config
    output_paths = {
        output_name.replace("export", "output"): path
        for output_name, path in config["files"].items()
        if "export" in output_name
    }

    root_output = config["general"]["output_dir"]

    # Use dictionary comprehension to create the selection list dict
    selection_dict = {
        dir[7:]: Path(f"{root_output}/{file}").with_suffix(".csv")
        for dir, file in output_paths.items()
        if file != "None"
    }

    # Log the files being exported
    logging.info(f"These are the files being exported: {selection_dict.values()}")

    return selection_dict


def check_files_exist(file_list: List, config: dict, isfile: callable):
    """Check that all the files in the file list exist using
    the imported isfile function."""

    # Check if the output dirs supplied are string, change to list if so

    platform = config["general"]["platform"]

    if isinstance(file_list, str):
        file_list = [file_list]

    # Check the existence of every file using is_file
    for file in file_list:
        file_path = Path(file)  # Changes to path if str
        OutgoingLogger.debug(f"Using {platform} isfile function")
        if not isfile(str(file_path)):
            OutgoingLogger.error(
                f"File {file} does not exist. Check existence and spelling"
            )
            raise FileNotFoundError(f"{file.name} not found in {file_path.parent}")
    OutgoingLogger.info("All output files exist")


def transfer_files(source, destination, method, logger, copy_files, move_files):
    """
    Transfer files from source to destination using the specified method and log
    the action.

    Args:
        source (str): The source file path.
        destination (str): The destination file path.
        method (str): The method to use for transferring files ("copy" or "move").
        logger (logging.Logger): The logger to use for logging the action.
    """
    transfer_func = {"copy": copy_files, "move": move_files}[method]
    past_tense = {"copy": "copied", "move": "moved"}[method]
    transfer_func(str(source), destination)

    logger.info(f"Files {source} successfully {past_tense} to {destination}.")


def get_username():
    """
    Retrieves the username of the currently logged-in user.

    This function uses the `getpass` module to get the username of the currently
    logged-in user.
    If the username cannot be determined, it defaults to "unknown".

    Returns:
        str: The username of the currently logged-in user, or "unknown" if the username
            cannot be determined.
    """
    # Get the user's username
    username = getpass.getuser()

    if username is None:
        username = "unknown"

    return username


def log_exports(
    list_file_exported: List, pipeline_run_datetime: datetime, logger: logging.Logger
):
    """
    Logs the details of the exported files.

    This function logs the date and time of the pipeline run, the username of the
    user who ran the pipeline, and the list of files that were exported. The date and
    time are formatted as "YYYY-MM-DD HH:MM:SS".

    Args:
        list_file_exported (List[str]): A list of the names of the files that were
            exported.
        pipeline_run_datetime (datetime): The date and time when the pipeline was run.
        logger (logging.Logger): The logger to use for logging the export details.

    Returns:
        None
    """
    # Get the user's username
    username = get_username()

    # Log the Date, time,username, and list of files
    pipeline_run_datetime = pipeline_run_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # Log the files being exported
    strs_exported_files = list(map(str, list_file_exported))
    logger.info(
        "{}: User {} exported the following files:\n{}".format(
            pipeline_run_datetime, username, "\n".join(strs_exported_files)
        )
    )


def run_export(export_config_path: str):
    """Main function to run the data export pipeline."""
    config = utils.load_config(export_config_path)

    # Check the environment switch
    platform = config["general"]["platform"]

    if platform == "s3":
        # create singletion boto3 client object & pass in bucket string
        from mbs_results.utilities.singleton_boto import SingletonBoto

        boto3_client = SingletonBoto.get_client(config)  # noqa
        from mbs_results.utilities import s3_mods as mods

    elif platform == "network":
        # If the platform is "network" or "hdfs", there is no need for a client.
        # Adding a client = None for consistency.
        config["client"] = None
        from mbs_results.utilities import local_file_mods as mods

    else:
        OutgoingLogger.error(f"The selected platform {platform} is wrong")
        raise ImportError(f"Cannot import {platform}_mods")

    OutgoingLogger.info(f"Using the {platform} file system as data source.")

    # Define paths
    paths = config["files"]  # Dynamically get paths based on config
    output_path = config["general"]["output_dir"]
    export_folder = config["general"]["export_dir"]

    # Get list of files to transfer from user
    file_select_dict = get_file_choice(paths, config)

    # Check that files exist
    check_files_exist(list(file_select_dict.values()), config, mods.rd_isfile)

    # Creating a manifest object using the Manifest class in manifest_output.py
    manifest = Manifest(
        outgoing_directory=output_path,
        export_directory=export_folder,
        pipeline_run_datetime=pipeline_run_datetime,
        dry_run=False,
        delete_file_func=mods.rd_delete_file,
        md5sum_func=mods.rd_md5sum,
        stat_size_func=mods.rd_stat_size,
        isdir_func=mods.rd_isdir,
        isfile_func=mods.rd_isfile,
        config=config,
        read_header_func=mods.rd_read_header,
        string_to_file_func=mods.rd_write_string_to_file,
    )

    schemas_header_dict = get_schema_headers(config, file_select_dict)

    # Add all output files to the manifest object
    for file_name, file_path in file_select_dict.items():
        manifest.add_file(
            file_path,
            column_header=schemas_header_dict[f"{file_name}"],
            validate_col_name_length=True,
            sep=",",
        )
    # Write the manifest file to the outgoing directory
    manifest.write_manifest()

    # Move the manifest file to the outgoing folder
    manifest_file = mods.rd_search_file(manifest.outgoing_directory, "_manifest.json")

    manifest_path = os.path.join(manifest.outgoing_directory, manifest_file)

    transfer_files(
        manifest_path,
        manifest.export_directory,
        "move",
        OutgoingLogger,
        mods.rd_copy_file,
        mods.rd_move_file,
    )

    # Copy or Move files to outgoing folder
    file_transfer_method = config["options"]["copy_or_move_files"]

    for file_path in file_select_dict.values():
        file_path = os.path.join(file_path)
        transfer_files(
            file_path,
            manifest.export_directory,
            file_transfer_method,
            OutgoingLogger,
            mods.rd_copy_file,
            mods.rd_move_file,
        )

    OutgoingLogger.info("Exporting files finished.")


if __name__ == "__main__":
    run_export(config=config_default)
