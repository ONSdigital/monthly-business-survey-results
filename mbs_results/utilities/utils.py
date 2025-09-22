import glob
import json
import os
import re
from importlib import metadata

import numpy as np
import pandas as pd
from pandas.io.json._table_schema import build_table_schema

from mbs_results import logger
from mbs_results.utilities.singleton_boto import SingletonBoto


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


def append_filter_out_questions(
    df: pd.DataFrame, filter_out_questions_path: str
) -> pd.DataFrame:
    """Appends data with question codes which were ommitted from the processing

    Parameters
    ----------
    df : pd.DataFrame
        Main dataframe to append.
    filter_out_questions_path : str
        File path with filtered out questions.

    Returns
    -------
    df : pd.DataFrame
       "Main dataframe with filtered-out questions. If the file is not found or empty,
        logs a FileNotFoundError error and returns original dataframe"

    """
    try:
        filter_out_questions_df = pd.read_csv(filter_out_questions_path)

        df = pd.concat([df, filter_out_questions_df])

    except FileNotFoundError:
        print(
            """File not found. Please check filter_out_questions_path path,
         filter_out_questions_df is being created by filter_out_questions()
         in mbs_results/staging/data_cleaning.py"""
        )

    return df


def get_versioned_filename(prefix, config):

    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    snapshot_name = os.path.basename(config["snapshot_file_path"].split(".")[0])
    filename = f"{prefix}_v{file_version_mbs}_{snapshot_name}.csv"

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

    if config["generate_schemas"] is True and config["schema_path"] is not None:

        output_p = config["output_path"]
        schema_p = config["schema_path"]

        logger.info("Generating schema files for output data...")

        # Create schemas using a local outputs folder, write them locally
        if config["platform"] == "network":
            output_files = glob.glob(f"{output_p}/*.csv")

            for file in output_files:
                df = pd.read_csv(file, low_memory=False)
                schema = build_table_schema(df, index=False, version=False)

                # Extract substring between last '/' or '\\' and first '.csv'
                filename = re.search(r"[^/\\]+(?=\.csv)", file)[0]

                logger.info(f"Generating schema for {filename}")

                with open(f"{schema_p}/{filename}_schema.json", "w") as f:
                    json.dump(schema, f, indent=4)

        # Create schemas using S3 bucket, write them to S3 bucket
        if config["platform"] == "s3":
            s3_client = SingletonBoto.get_client(config)
            s3_bucket = SingletonBoto.get_bucket()

            output_response = s3_client.list_objects(Bucket=s3_bucket, Prefix=output_p)

            for content in output_response["Contents"]:
                file_key = content["Key"]
                if file_key.endswith(".csv"):
                    csv_response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
                    df = pd.read_csv(csv_response["Body"], low_memory=False)

                    schema = build_table_schema(df, index=False, version=False)

                    # Extract substring between last '/' or '\\' and first '.csv'
                    filename = re.search(r"[^/\\]+(?=\.csv)", file_key)[0]

                    logger.info(f"Generating schema for {filename}")

                    s3_client.put_object(
                        Body=json.dump(schema, indent=4),
                        Bucket=s3_bucket,
                        Key=f"{schema_p}/{filename}_schema.json",
                    )
    else:
        logger.info("Schema generation not enabled in config, skipping...")
