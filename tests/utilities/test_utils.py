from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import toml
from pandas.testing import assert_frame_equal

from mbs_results.utilities.inputs import read_colon_separated_file
from mbs_results.utilities.utils import (
    check_above_one,
    check_duplicates,
    check_input_types,
    check_missing_values,
    check_non_negative,
    check_population_sample,
    check_unique_per_cell_period,
    compare_two_dataframes,
    generate_schemas,
    get_or_create_run_id,
    get_or_read_run_id,
    unpack_dates_and_comments,
    multi_filter_list,
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


@pytest.fixture(scope="class")
def config(filepath):
    return {
        "platform": "network",
        "output_path": filepath,
        "schema_path": filepath,
        "generate_schemas": True,
    }


class TestGenerateSchemas:
    def test_generate_schemas_no_error(self, config):
        generate_schemas(config)

        schema = toml.load(config["schema_path"] + "non_empty_dataset_schema.toml")

        assert schema == {
            "variable_1": {"old_name": "variable_1", "Deduced_Data_Type": "object"},
            "variable_2": {"old_name": "variable_2", "Deduced_Data_Type": "int64"},
            "variable_3": {"old_name": "variable_3", "Deduced_Data_Type": "float64"},
            "variable_4": {"old_name": "variable_4", "Deduced_Data_Type": "bool"},
        }

    def test_generate_schemas_unversioned_name(self, config):
        generate_schemas(config)

        schema = toml.load(config["schema_path"] + "versioned_name_schema.toml")

        assert schema


class TestRunIDFunctions:
    def test_get_or_create_run_id(self):
        with patch(
            "mbs_results.utilities.utils.get_datetime_now_as_int", return_value=1234
        ):
            result_empty = get_or_create_run_id({"run_id": ""})
            result_populated = get_or_create_run_id({"run_id": 5678})
        assert result_empty == 1234
        assert result_populated == 5678

    def test_get_or_read_run_id(self, config):
        with patch("mbs_results.utilities.utils.read_run_id", return_value=4321):
            result_empty = get_or_read_run_id({"run_id": ""})
            result_populated = get_or_read_run_id({"run_id": 5678})
        assert result_empty == 4321
        assert result_populated == 5678


def test_unpack_dates_and_comments():
    df = pd.DataFrame(
        {
            "period": [2023, 2023, 2023, 2023],
            "reference": [1, 1, 1, 1],
            "question_no": [11, 12, 146, 40],
            "adjustedresponse": [None, None, None, 200.0],
            "response": ["2023-01-01", "2023-01-02", "This is a comment", "210.0"],
        }
    )
    config = {
        "filter_out_questions": [11, 12, 146],
        "question_no_plaintext": {"11": "start", "12": "end", "146": "comments"},
        "period": "period",
        "reference": "reference",
        "question_no": "question_no",
        "target": "adjustedresponse",
    }

    result = unpack_dates_and_comments(
        df=df,
        reformat_questions=config["filter_out_questions"],
        question_no_plaintext=config["question_no_plaintext"],
        config=config,
    )
    expected = pd.DataFrame(
        {
            "period": [2023],
            "reference": [1],
            "question_no": [40],
            "adjustedresponse": [200.0],
            "response": ["210.0"],
            "start": ["2023-01-01"],
            "end": ["2023-01-02"],
            "comments": ["This is a comment"],
        }
    )
    assert_frame_equal(result, expected)

@pytest.fixture(scope="class")
def input_list():
    return ["test123", "test456", "not789"]


class TestMultiFilterList:
    def test_no_args(self, input_list):

        result = multi_filter_list(input_list)

        assert result == []

    def test_one_args(self, input_list):

        result = multi_filter_list(input_list, "test")

        assert result == ["test123", "test456"]

    def test_two_args(self, input_list):

        result = multi_filter_list(input_list, "test", "123")

        assert result == ["test123"]

    def test_no_matches(self, input_list):

        result = multi_filter_list(input_list, "test", "759")

        assert result == []
