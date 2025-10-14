import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
import toml

from mbs_results.utilities.inputs import read_colon_separated_file
from mbs_results.utilities.utils import (
    generate_schemas,
    check_above_one,
    check_duplicates,
    check_input_types,
    check_missing_values,
    check_non_negative,
    check_population_sample,
    check_unique_per_cell_period,
    compare_two_dataframes,
)


def test_read_colon_separated_file(utilities_data_dir):
    headers = ["int", "str", "float", "period"]
    expected = pd.DataFrame(
        {
            "int": [1, 2, 3],
            "str": ["A", "B", "C"],
            "float": [1.0, 2.0, 3.0],
            "period": [202401, 202401, 202401],
        }
    )

    actual = read_colon_separated_file(
        utilities_data_dir / "read_colon_separated_file/colon_sep_202401", headers
    )

    assert_frame_equal(actual, expected)


def test_compare_two_dataframes(utilities_data_dir):
    df1 = pd.read_csv(utilities_data_dir / "utils" / "compare_two_dataframes_input.csv")
    df2 = df1.copy()

    # Modifying some rows in df2
    df2.loc[0:4, "frotover"] = [1000, 2000, 3000, 4000, 5000]
    df2.loc[0:4, "imputation_flags_adjustedresponse"] = ["mc", "mc", "mc", "mc", "mc"]

    # Creating the diff based on modified rows
    df1_diff = df1.copy().loc[0:4]
    df2_diff = df2.copy().loc[0:4]

    df1_diff["version"] = "df1"
    df2_diff["version"] = "df2"

    expected_diff = pd.concat([df1_diff, df2_diff])
    expected_column_diff = ["imputation_flags_adjustedresponse", "frotover"]

    actual_diff, actual_column_diff = compare_two_dataframes(df1, df2)

    assert_frame_equal(expected_diff, actual_diff)

    assert expected_column_diff == actual_column_diff


def test_check_duplicates_raises_value_error():

    df = pd.DataFrame({"col1": [1, 2, 2], "col2": [3, 4, 4]})
    columns = ["col1", "col2"]

    with pytest.raises(ValueError, match="Duplicate rows found based on columns:"):
        check_duplicates(df, columns)


def test_check_missing_values_raises_value_error():

    df = pd.DataFrame({"col1": [1, 2, 3]})
    column = "col2"

    with pytest.raises(ValueError, match="Missing required column: col2"):
        check_missing_values(df, column)

    df = pd.DataFrame({"col1": [1, None, 3]})
    column = "col1"

    with pytest.raises(ValueError, match="Column col1 contains missing values."):
        check_missing_values(df, column)


def test_check_input_types_raises_value_error():
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    expected_types = {"col1": np.integer, "col2": np.float64}

    with pytest.raises(ValueError, match="Missing required column: col3"):
        check_input_types(df, {"col3": np.float64})

    with pytest.raises(
        TypeError, match="Column col2 is not of type <class 'numpy.float64'>"
    ):
        check_input_types(df, expected_types)


def test_check_population_sample_raises_value_error():

    df = pd.DataFrame(
        {
            "population": [1, 2, 3],
            "sample": [1, 2, 3],
            "a": [1, 0, 1],
            "g": [1, 1, 0],
        }
    )

    with pytest.raises(
        ValueError, match="If population = sample, all refs must have a = 1"
    ):
        check_population_sample(df, "population", "sample")

    df["a"] = [1, 1, 1]
    with pytest.raises(
        ValueError, match="If population = sample, all refs must have g = 1"
    ):
        check_population_sample(df, "population", "sample")


def test_check_unique_per_cell_period_raises_value_error():

    df = pd.DataFrame({"cell": [1, 2], "period": [2023, 2023]})

    with pytest.raises(ValueError, match="Missing required weight column: weight"):
        check_unique_per_cell_period(df, "cell", "period", "weight")

    df = pd.DataFrame(
        {
            "cell": [1, 1, 2],
            "period": [2023, 2023, 2023],
            "weight": [1.0, 2.0, 3.0],
        }
    )

    with pytest.raises(
        ValueError, match="Multiple unique values found for weight in each cell/period"
    ):
        check_unique_per_cell_period(df, "cell", "period", "weight")


def test_check_non_negative_raises_value_error():

    df = pd.DataFrame({"col1": [1, -2, 3]})
    column = "col1"

    with pytest.raises(ValueError, match="Column col1 contains negative values."):
        check_non_negative(df, column)


def test_check_above_one_raises_value_error():

    df = pd.DataFrame({"col1": [0.5, 1.0, 1.5]})
    column = "col1"

    with pytest.raises(
        ValueError, match="Column col1 contains values not greater than 1."
    ):
        check_above_one(df, column)


@pytest.fixture(scope="class")
def filepath():
    return "tests/data/utilities/utils/generate_schemas/"


class TestGenerateSchemas:
    
    def test_generate_schemas_no_error(self, filepath):
        config = {
            "platform": "network",
            "output_path": filepath,
            "schema_path": filepath,
            "generate_schemas": True,
        }

        generate_schemas(config)

        schema = toml.load(config["schema_path"] + "non_empty_dataset_schema.toml")

        assert schema == {
            "variable_1": {"old_name": "variable_1", "Deduced_Data_Type": "object"},
            "variable_2": {"old_name": "variable_2", "Deduced_Data_Type": "int64"},
            "variable_3": {"old_name": "variable_3", "Deduced_Data_Type": "float64"},
            "variable_4": {"old_name": "variable_4", "Deduced_Data_Type": "bool"},
        }
