import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

# set up logging
ManifestLogger = logging.getLogger(__name__)


class ManifestError(Exception):
    pass


class Manifest:
    """
    An outgoing file transfer manifest. Accumulates file metadata before
        writing a JSON manifest file.

    Used by Apache NiFi for data integrity checks before initiating data
        transfers.

    Attributes
    ==========
    outgoing_directory
        location to write files to
    pipeline_run_datetime
        datetime of the current pipeline run, used to version outputs
    dry_run
        when True, cleans up output files after a successful run
    """

    def __init__(
        self,
        outgoing_directory: str,
        export_directory: str,
        pipeline_run_datetime: datetime,
        delete_file_func: callable,
        md5sum_func: callable,
        stat_size_func: callable,
        isdir_func: callable,
        isfile_func: callable,
        read_header_func: callable,
        string_to_file_func: callable,
        config: Dict[str, Any],
        dry_run: bool = False,
        delete_on_fail=False,
    ):
        self.outgoing_directory = outgoing_directory
        self.export_directory = export_directory
        if not isdir_func(outgoing_directory):
            raise ManifestError(
                f"Outgoing directory does not exist: {self.outgoing_directory}"
            )
        if not isdir_func(export_directory):
            raise ManifestError(
                f"Export directory does not exist: {self.export_directory}"
            )

        if not isinstance(pipeline_run_datetime, datetime):
            raise ManifestError(
                "The datetime of the Pipeline run must be a datetime object."
                "The data type is wrong."
            )

        self.manifest_datetime = pipeline_run_datetime.strftime("%Y%m%d_%H%M")
        self.manifest_filename = self.manifest_datetime

        # filename = filename_amender(
        #     filename="metadata_manifest",
        #     config=config
        # )
        # filename = filename.replace("csv", "json")

        self.manifest_file_path = os.path.join(
            outgoing_directory, (self.manifest_datetime + "_metadata_manifest.json")
        )
        self.manifest: dict = {"files": []}
        self.written = False
        self.invalid_headers: list = []
        self.dry_run = dry_run
        self.delete_on_fail = delete_on_fail

        # Functions
        self.delete_file = delete_file_func
        self.md5sum = md5sum_func
        self.stat_size = stat_size_func
        self.isdir = isdir_func
        self.isfile = isfile_func
        self.read_header = read_header_func
        self.string_to_file = string_to_file_func

    def add_file(
        self,
        relative_file_path: str,
        column_header: str = "",
        validate_col_name_length: bool = False,
        sep: str = ",",
    ):
        """
        Add a file in the outgoing folder to the manifest.
        The file must exist in a subdirectory of the manifest `outgoing_directory`.

        Parameters
        ----------
        relative_file_path
            from outgoing directory to the file that you want to add to the manifest
        column_header
            the exact column header string
        """
        # if "outputs" not in str(relative_file_path):
        #     raise ManifestError(
        #         f"""File must be in a subdirectory of the outgoing directory:
        #             {relative_file_path}"""
        #     )

        absolute_file_path = os.path.join(
            self.outgoing_directory, os.path.basename(relative_file_path)
        )

        if not self.isfile(absolute_file_path):
            raise ManifestError(
                f"""Cannot add file to manifest, file does not exist:
                    {absolute_file_path}"""
            )
        # By default write an empty string in manifest
        # Otherwise check columns

        if column_header != "":
            # Get the col headers from the file
            file_header_string = self.read_header(absolute_file_path)

            # Cleanup file_header_list because \n is appearing in it
            file_header_string = file_header_string.replace("\n", "")

            file_header_list = file_header_string.split(sep)
            column_header_list = column_header.split(sep)
            if file_header_list != column_header_list:
                # Column headers in file do not match expected column headers

                # Compare strings `true_header_string` and `column_header`
                # Generate a report of the differences between them.
                self.invalid_headers.append(
                    f"File:{absolute_file_path}\n"
                    f"Expected:     {column_header}\n"
                    f"Got:          {file_header_string}\n"
                    f"Missing from file: {set(column_header_list) - set(file_header_list)}\n"  # noqa
                    f"Missing from schema: {set(file_header_list) - set(column_header_list)}\n"  # noqa
                )

            else:
                # Column headers in file match expected column headers
                ManifestLogger.info(
                    f"Column headers match for {relative_file_path} and its schema."
                )

            # check for empty column headers
            any_empty_strings = bool([n.strip() for n in file_header_list if n == ""])
            if (
                column_header.strip() == ""
                or file_header_string.strip() == ""
                or any_empty_strings
            ):
                # Raise error if headers are an empty string
                raise ManifestError("Column headers cannot be an empty string.")

        # Checks that column names are not more than 32 chars
        if validate_col_name_length:
            col_above_max_len = [head for head in file_header_list if len(head) > 32]

            if len(col_above_max_len) > 0:
                self.invalid_headers.append(
                    f"File:{absolute_file_path}\n"
                    "These column names are exceeding the maximum char length "
                    "of 32: {col_above_max_len}\n"
                )
        # Check that files are not more than 2.5Gb as nifi can't cope
        file_size_bytes = int(self.stat_size(absolute_file_path))
        file_size_gb = file_size_bytes / 1024**3
        if file_size_gb > 2.5:
            raise ManifestError(
                f"""Error with {absolute_file_path}
                    File size is too big"""
            )

        # Remove leading slashes from the relative directory path
        relative_dir_str = str(os.path.dirname(relative_file_path))
        while relative_dir_str.startswith("/"):
            relative_dir_str = relative_dir_str[1:]

        file_manifest = {
            "file": os.path.basename(relative_file_path),
            "subfolder": self.export_directory,
            "sizeBytes": file_size_bytes,
            "md5sum": self.md5sum(absolute_file_path),
            "header": column_header,
        }
        self.manifest["files"].append(file_manifest)

    def write_manifest(self):
        """
        Write outgoing file manifest to JSON in HDFS.
        A manifest can only be written once during pipeline run.
        """
        any_invalid_headers = len(self.invalid_headers) > 0

        if any_invalid_headers and self.delete_on_fail:
            self._delete_files_after_fail()
            raise ManifestError("\n".join(self.invalid_headers))

        if self.written:
            raise ManifestError("Manifest has already been written.")

        if len(self.manifest["files"]) < 1:
            raise ManifestError("Can't write an empty Manifest.")

        if self.dry_run:
            self._delete_files_after_fail()
            self.written = True
            return

        self.string_to_file(
            json.dumps(self.manifest, indent=4).encode("utf-8"), self.manifest_file_path
        )
        self.written = True

    def _delete_files_after_fail(self):
        """
        Delete all files in the manifest. Used when manifest content does not
        match the target file for transfer, or during a dry run.
        """

        for f in self.manifest["files"]:
            absolute_path = os.path.join(
                self.outgoing_directory, f["subfolder"], f["file"]
            )
            if self.isfile(absolute_path):
                self.delete_file(absolute_path)
