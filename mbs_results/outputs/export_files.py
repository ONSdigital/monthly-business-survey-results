"""This is a stand alone pipeline to selectively transfer output files from
the output folder to the outgoing folder, along with their manifest file."""

import getpass
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List
import boto3
import raz_client
import tomli
from rdsa_utils.cdp.helpers.s3_utils import list_files
import itertools

import mbs_results.utilities.merge_two_config_files as utils
from mbs_results.utilities.manifest_output import Manifest
from mbs_results.utilities.utils import multi_filter_list,get_or_read_run_id
# Set up logging

OutgoingLogger = logging.getLogger(__name__)

# Setting logging levels
warning_only = ["raz_client_logger", "requests_kerberos.kerberos_", "spnego._gss"]
for module in warning_only:
    logging.getLogger(module).setLevel(logging.WARNING)


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


def get_file_choice(config:dict)->dict:
    """
    Creates a list of files found based on user's choices from the configuration.
    
    User must supply a dictionary ('files_to_export' field in config) with bool
    values (true if they want to export). And a dictionary with basenames (
    'files_basename' field in exports).
    
    This function will list of the files which were found in the config 
    `output_dir` which satisfies the criteria.
    
    Parameters
    ----------
    config : dict
        The configuration. More particularly 
        `output_dir`: folder path to data 
        `platform`: the platform (s3 for AWS)
        `bucket`: the s3 bucket if s3 platform is selected
        `files_to_export`: a dictionary with bool values indicationg which 
        files should be found
        `files_basename`: a dictionary with mapping between files and their
        base names.

    Raises
    ------
    ValueError
        If `files_to_export` keys don't match `files_basename` keys.

    Returns
    -------
    dict
       A dictionary with all files found in respect with the
       relevant key.
    """
    
    # Use rdsa utils for s3 otherwise use default os
    
    if config["platform"]=="s3":
        client = boto3.client("s3")
        raz_client.configure_ranger_raz(
            client, ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
        )
        
        
        all_files = list_files(client,  config["bucket"], config["output_dir"])
      
    else:
        all_files = os.listdir()
                
    run_id = get_or_read_run_id(config)

    if config["files_to_export"].keys() != config["files_basename"].keys():
      
      raise ValueError("""
      Keys in config field 'files_to_export' must be the same with 'files_basename'""")

    user_choice = {
      k : v for k,v in config["files_basename"].items() if config["files_to_export"].get(k)}
            
    for_export = {k: multi_filter_list(all_files,basename,run_id) for k,basename in user_choice.items()}

    # Log the files being exported
    logging.info(f"These are the files being exported: {for_export}")

    return for_export


def check_files_exist(file_list: List, config: dict, isfile: callable):
    """Check that all the files in the file list exist using
    the imported isfile function."""

    # Check if the output dirs supplied are string, change to list if so

    platform = config["platform"]

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
    platform = config["platform"]

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
    output_path = config["output_dir"]
    export_folder = config["export_dir"]

    # Get list of files to transfer from user
    file_select_dict = get_file_choice(config)
    
    files_found = list(itertools.chain(*file_select_dict.values()))

    # Check that files exist
    check_files_exist(files_found, config, mods.rd_isfile)

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
    # get headers from schemas and validate columns when user supplies a schema
    if config["schemas_dir"]:

        schemas_header_dict = get_schema_headers(config, file_select_dict)
        validate_col_name_length_bool = True

    # Default,  headers will be "" in manifest file if no schemas in config
    else:
        schemas_header_dict = {key: "" for key in file_select_dict.keys()}
        validate_col_name_length_bool = False

    # Add all output files to the manifest object
    # file_select_dict has a list of relevant paths
    for file_name, list_files in file_select_dict.items():
      for file_path in list_files:
        manifest.add_file(
            file_path,
            column_header=schemas_header_dict[f"{file_name}"],
            validate_col_name_length=validate_col_name_length_bool,
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
    file_transfer_method = config["copy_or_move_files"]

    for file_path in files_found:
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
