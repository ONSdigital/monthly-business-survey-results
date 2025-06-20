from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from mbs_results.staging.data_cleaning import (
    clean_and_merge,
    convert_cell_number,
    convert_nil_values,
    create_imputation_class,
    enforce_datatypes,
    filter_out_questions,
    is_census,
    run_live_or_frozen,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/data_cleaning")


def correct_types(df):
    df_expected_out = df.copy()
    df_expected_out["period"] = pd.to_datetime(df["period"], format="%Y%m")
    df_expected_out["strata"] = df_expected_out["strata"].astype("str")
    df_expected_out["reference"] = df_expected_out["reference"].astype("int64")
    df_expected_out["target_variable"] = df_expected_out["target_variable"].astype(
        "float"
    )
    return df_expected_out


def test_enforce_datatypes(filepath):
    df = pd.read_csv(filepath / "imputation_flag_data.csv")
    df_subset = df[["period", "strata", "reference", "target_variable"]]
    expected_output = correct_types(df_subset)
    df_subset = df_subset.set_index(["reference", "period"])
    test_setup_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": ["period", "strata", "reference", "target_variable"],
        "master_column_type_dict": {
            "period": "date",
            "strata": "str",
            "reference": "int",
            "target_variable": "float",
            "unused_col": "float",
        },
        "temporarily_remove_cols": [],
    }
    actual_output = enforce_datatypes(
        df_subset,
        keep_columns=test_setup_config["responses_keep_cols"],
        **test_setup_config
    )
    actual_output = actual_output.reindex(sorted(actual_output.columns), axis=1)
    expected_output = expected_output.reindex(sorted(expected_output.columns), axis=1)
    assert_frame_equal(actual_output, expected_output)


def test_clean_and_merge():
    test_setup_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {
            "period": "date",
            "reference": "int",
            "target_variable": "float",
        },
        "contributors_keep_cols": {
            "reference": "int",
            "period": "date",
            "strata": "str",
        },
        "temporarily_remove_cols": [],
    }
    snapshot = {
        "responses": [
            {
                "reference": "1",
                "period": "202212",
                "target_variable": "20",
                "lastupdateddate": "2024-06-28 00:00:01.999999+00",
            }
        ],
        "contributors": [
            {"reference": "1", "period": "202212", "survey": "202", "strata": "101"}
        ],
    }
    actual_output = clean_and_merge(snapshot, **test_setup_config)
    dictionary_data = {
        "reference": ["1"],
        "period": ["202212"],
        "target_variable": ["20"],
        "strata": ["101"],
    }
    expected_output = pd.DataFrame(data=dictionary_data).set_index(
        ["reference", "period"]
    )
    assert_frame_equal(actual_output, expected_output)


def test_create_imputation_class(filepath):

    expected_output = pd.read_csv(filepath / "test_create_imputation_class.csv")

    df_in = expected_output.drop(columns=["expected"])

    actual_output = create_imputation_class(df_in, "cell_no", "expected")
    assert_frame_equal(actual_output, expected_output)


def test_convert_cell_number(filepath):

    df = pd.read_csv(filepath / "test_convert_cell_number.csv")

    df_in = df.drop(columns=["expected"])

    actual_output = convert_cell_number(df_in, "cell_number")

    expected_output = df.drop(columns=["cell_number"]).rename(
        columns={"expected": "cell_number"}
    )

    assert_frame_equal(actual_output, expected_output)


def test_run_live_or_frozen(filepath):

    df_in = pd.read_csv(filepath / "test_run_live_or_frozen_input.csv")

    expected_frozen_output = pd.read_csv(
        filepath / "test_run_live_or_frozen_frozen_output.csv"
    )

    expected_live_output = df_in.copy()

    live_ouput = run_live_or_frozen(df_in, "target", "status", "live")

    frozen_output = run_live_or_frozen(df_in, "target", "status", "frozen")

    assert_frame_equal(frozen_output, expected_frozen_output)
    assert_frame_equal(live_ouput, expected_live_output)


def test_run_live_or_frozen_exception(filepath):

    df = pd.read_csv(filepath / "test_run_live_or_frozen_input.csv")

    with pytest.raises(ValueError):
        run_live_or_frozen(df, "target", "error", "love")


def test_is_census(filepath):

    df = pd.read_csv(filepath / "test_is_cencus.csv")

    extra_cal_groups = [
        5043,
        5113,
        5123,
        5203,
        5233,
        5403,
        5643,
        5763,
        5783,
        5903,
        6073,
    ]

    input_series = df["calibration_group"]
    expected_output = df["is_census"]

    # By default takes name of input series
    actual_output = is_census(input_series, extra_cal_groups)
    actual_output.name = "is_census"

    assert_series_equal(actual_output, expected_output)


@patch("pandas.DataFrame.to_csv")  # mock pandas export csv function
def test_filter_out_questions(mock_to_csv, filepath):

    df_in = pd.read_csv(filepath / "test_filter_out_questions.csv")
    expected_output = pd.read_csv(filepath / "test_filter_out_questions_expected.csv")

    questions_to_remove = [11, 12, 146]

    network_config = {"platform": "network", "bucket": ""}

    actual_output = filter_out_questions(
        df_in, "question", questions_to_remove, "export.csv", **network_config
    )

    # testing if pandas export was called once
    mock_to_csv.assert_called_once_with("export.csv", index=False, date_format="%Y%m")

    assert_frame_equal(actual_output, expected_output)


def test_filter_out_questions_save_full_path_errror():
    """Check if ValueError raised when path is not csv"""

    with pytest.raises(ValueError):
        filter_out_questions("dummy", "dummy", "dummy", "export.txt")


def test_convert_nil_values(filepath):

    nil_values = [
        "Combined child (NIL2)",
        "Out of scope (NIL3)",
        "Ceased trading (NIL4)",
        "Dormant (NIL5)",
        "Part year return (NIL8)",
        "No UK activity (NIL9)",
    ]

    df_in = pd.read_csv(filepath / "test_convert_nil_values_input.csv")
    expected_output = pd.read_csv(filepath / "test_convert_nil_values_output.csv")

    actual_output = convert_nil_values(df_in, "status", "value", nil_values)

    assert_frame_equal(actual_output, expected_output)


def test_convert_nil_values_warning(filepath):
    """Test if warning is working"""

    nil_values = [
        "Combined child (NIL2)",
        "Out of scope (NIL3)",
        "Ceased trading (NIL4)",
        "Dormant (NIL5)",
        "Part year return (NIL8)",
        "No UK activity (NIL9)",
    ]

    df_in_warning = pd.read_csv(filepath / "test_convert_nil_values_input_warning.csv")

    with pytest.warns():
        convert_nil_values(df_in_warning, "status", "value", nil_values)
