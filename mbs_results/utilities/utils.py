import datetime
import glob
import logging
import re

import numpy as np
import pandas as pd
import toml

from mbs_results.utilities.singleton_boto import SingletonBoto

logger = logging.getLogger(__name__)


def convert_column_to_datetime(dates):
    """
    Convert string pandas series to datetime (from raw inputs).

    Parameters
    ----------
    dates : pd.Series.

    Returns
    -------
    df : pd.Series
    """
    return pd.to_datetime(dates, format="%Y%m")


def get_versioned_filename(prefix, run_id):

    filename = f"{prefix}_{run_id}.csv"

    return filename


def compare_two_dataframes(df1, df2):
    """
    Compare two dataframes and identify the differences between them.

    Parameters:
    - df1: pd.DataFrame
        The first dataframe to compare.
    - df2: pd.DataFrame
        The second dataframe to compare.

    Returns:
    - diff: pd.DataFrame
        A dataframe containing the rows that are different between df1 and df2.
    - changed_columns: list
        A list of column names that have differences between df1 and df2.
    """
    all_columns = df1.columns.union(df2.columns)
    df1_aligned = df1.reindex(columns=all_columns)
    df2_aligned = df2.reindex(columns=all_columns)

    df1_aligned["version"] = "df1"
    df2_aligned["version"] = "df2"

    diff = pd.concat([df1_aligned, df2_aligned]).drop_duplicates(
        subset=all_columns, keep=False
    )

    changed_columns = []
    for column in all_columns:
        if not df1_aligned[column].equals(df2_aligned[column]):
            changed_columns.append(column)

    return diff, changed_columns


def check_duplicates(df, columns):
    """Check for duplicate rows based on specified columns."""
    if df.duplicated(subset=columns).any():
        raise ValueError(f"Duplicate rows found based on columns: {columns}")


def check_missing_values(df, column):
    """Check for missing values in a specific column."""
    if column not in df.columns:
        raise ValueError(f"Missing required column: {column}")
    if df[column].isnull().any():
        raise ValueError(f"Column {column} contains missing values.")


def check_input_types(df, expected_types):
    """Check types of input variables."""
    for column, expected_type in expected_types.items():
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}")
        if not np.issubdtype(df[column].dtype, expected_type):
            raise TypeError(f"Column {column} is not of type {expected_type}")


def check_population_sample(df, population_column, sample_column):
    """Check if population = sample, then a and g should be 1."""
    if (df[population_column] == df[sample_column]).all():
        if not (df["a"] == 1).all():
            raise ValueError("If population = sample, all refs must have a = 1")
        if not (df["g"] == 1).all():
            raise ValueError("If population = sample, all refs must have g = 1")


def check_unique_per_cell_period(df, cell_column, period_column, column):
    """Check that for each cell/period there is only one unique design weight."""
    for col in [cell_column, period_column, column]:
        if col not in df.columns:
            raise ValueError(f"Missing required weight column: {col}")
    if df.groupby([cell_column, period_column])[column].nunique().max() > 1:
        raise ValueError(
            f"Multiple unique values found for {column} in each cell/period"
        )


def check_non_negative(df, column):
    """Check that all values in a column are non-negative."""
    if column not in df.columns:
        raise ValueError(f"Missing required column: {column}")
    if (df[column] < 0).any():
        raise ValueError(f"Column {column} contains negative values.")


def check_above_one(df, column):
    """Check that all values in a column are non-negative."""
    if column not in df.columns:
        raise ValueError(f"Missing required column: {column}")
    if (df[column] < 1).any():
        raise ValueError(f"Column {column} contains values not greater than 1.")


# flake8: noqa: C901
def generate_schemas(config):
    """
    Generate schema files for output data.

    Parameters
    ----------

    config : dict
        Configuration dictionary containing paths for output and schema folders.

    Returns
    -------
    None
    """

    if config["generate_schemas"] and config["schema_path"]:

        output_p = config["output_path"]
        schema_p = config["schema_path"]

        logger.info("Generating schema files for output data...")

        # Create schemas using a local outputs folder, write them locally
        if config["platform"] == "network":
            output_files = glob.glob(f"{output_p}/*.csv")

            for file in output_files:
                try:
                    df = pd.read_csv(file, low_memory=False)
                    schema = build_toml_schema(df)

                    # Extract substring between last '/' or '\\' and first '.csv'
                    filename = re.search(r"[^/\\]+(?=\.csv)", file)[0]

                    filename = de_version_filename(filename)

                    logger.info(f"Generating schema for {filename}")

                    with open(f"{schema_p}/{filename}_schema.toml", "w") as f:
                        toml.dump(schema, f)
                except pd.errors.EmptyDataError:
                    logger.warning(f"Skipping schema for empty file: {file}")

        # Create schemas using S3 bucket, write them to S3 bucket
        if config["platform"] == "s3":
            s3_client = SingletonBoto.get_client(config)
            s3_bucket = SingletonBoto.get_bucket()

            output_response = s3_client.list_objects(Bucket=s3_bucket, Prefix=output_p)

            for content in output_response["Contents"]:
                file_key = content["Key"]

                if file_key.endswith(".csv"):
                    csv_response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)

                    try:
                        df = pd.read_csv(csv_response["Body"], low_memory=False)

                        schema = build_toml_schema(df)

                        # Extract substring between last '/' or '\\' and first '.csv'
                        filename = re.search(r"[^/\\]+(?=\.csv)", file_key)[0]

                        filename = de_version_filename(filename)

                        logger.info(f"Generating schema for {filename}")

                        s3_client.put_object(
                            Body=toml.dump(schema),
                            Bucket=s3_bucket,
                            Key=f"{schema_p}/{filename}_schema.json",
                        )
                    except pd.errors.EmptyDataError:
                        logger.warning(f"Skipping schema for empty file: {file_key}")

    else:
        logger.info("Schema generation not enabled in config, skipping...")


def build_toml_schema(df: pd.DataFrame) -> dict:
    """
    Build a dict ready for conversion into a TOML schema

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to build the schema from.

    Returns
    -------
    schema : dict
        A dictionary representing the schema of the DataFrame.
    """
    schema = {}

    for name, values in df.items():
        schema.update(
            {name: {"old_name": name, "Deduced_Data_Type": str(values.dtype)}}
        )

    return schema


def de_version_filename(filename: str) -> str:
    """
    De-version a filename by removing version information.

    Parameters
    ----------
    filename : str
        The versioned filename.

    Returns
    -------
    de_versioned_filename : str
        The de-versioned filename.
    """
    # Regex to remove version information (e.g., v1.0.0)
    filename = re.sub(r"_?v\d+(\.\d+)*", "", filename)

    # Regex to remove snapshot information (e.g., snapshot_009_202301_1)
    filename = re.sub(r"_?snapshot_?(qv_cp)?(_\d+)?_(\d+)?(_\d+)?", "", filename)

    # Regex to remove quarter (e.g., 2023Q1)
    filename = re.sub(r"_?\d+Q[1-4]", "", filename)

    return filename


def get_datetime_now_as_int():
    """Returns datetime up to minutes as an integer.

    Returns
    -------
    now_as_int : int
        Datetime now as integer.

    """

    now = datetime.datetime.now()

    now_as_int = int(now.strftime("%Y%m%d%H%M"))

    return now_as_int


def export_run_id(value: str):
    """Save a value in .RUN_ID text file.

    Parameters
    ----------
    value : str
        Value to export.

    Returns
    -------
    None.

    """

    with open(".RUN_ID", "w") as file:
        file.write(str(value))


def read_run_id() -> str:
    """Read a value from .RUN_ID text file.

    Parameters
    ----------

    Returns
    -------
    A string value.

    """

    file = open(".RUN_ID", "r").readlines()

    if len(file) != 1:
        raise ValueError(".RUN_ID must be one line")

    return file[0]


def get_or_create_run_id(config: dict) -> int:
    """
    gets run id from config or creates a new one based on current datatime

    Parameters
    ----------
    config : dict
        dictionary containing configuration parameters, run_id does not need to be
        present.

    Returns
    -------
    int
        The run ID.
    """
    run_id = config.get("run_id") or get_datetime_now_as_int()
    return run_id


def get_or_read_run_id(config: dict) -> int:
    """
    Gets run ID from config or reads it from .RUN_ID file.

    Parameters
    ----------
    config : dict
        Dictionary containing configuration parameters, run_id does not need to be
        present.

    Returns
    -------
    int
        The run ID.
    """
    run_id = config.get("run_id") or read_run_id()
    return run_id
