from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.data_cleaning import (
    clean_and_merge,
    create_imputation_class,
    enforce_datatypes,
    run_live_or_frozen,
)


def correct_types(df):
    df_expected_out = df.copy()
    df_expected_out["period"] = pd.to_datetime(df["period"], format="%Y%m")
    df_expected_out["strata"] = df_expected_out["strata"].astype("str")
    df_expected_out["reference"] = df_expected_out["reference"].astype("int")
    df_expected_out["target_variable"] = df_expected_out["target_variable"].astype(
        "float"
    )
    df_expected_out = df_expected_out.set_index(["reference", "period"])
    return df_expected_out


def test_enforce_datatypes():
    df = pd.read_csv(Path("tests") / "imputation_flag_data.csv")
    df_subset = df[["period", "strata", "reference", "target_variable"]]
    expected_output = correct_types(df_subset)
    df_subset = df_subset.set_index(["reference", "period"])
    test_setup_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {"period": "date", "strata": "str"},
        "contributors_keep_cols": {"reference": "int", "target_variable": "float"},
        "temporarily_remove_cols": [],
    }
    actual_output = enforce_datatypes(df_subset, **test_setup_config)

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


def test_create_imputation_class():

    expected_output = pd.read_csv(Path("tests") / "test_create_imputation_class.csv")

    df_in = expected_output.drop(columns=["expected"])

    actual_output = create_imputation_class(df_in, "cell_no", "expected")

    assert_frame_equal(actual_output, expected_output)


def test_run_live_or_frozen():

    df = pd.read_csv(Path("tests") / "test_run_live_or_frozen.csv")

    df_in = df.drop(columns=["frozen"])

    live_ouput = run_live_or_frozen(df_in, "target", "error", "live")
    frozen_output = run_live_or_frozen(df_in, "target", "error", "frozen")

    expected_output_frozen = df_in.copy()
    expected_output_frozen["target"] = df["frozen"]

    assert_frame_equal(frozen_output, expected_output_frozen)
    assert_frame_equal(live_ouput, df_in)


def test_run_live_or_frozen_exception():

    df = pd.read_csv(Path("tests") / "test_run_live_or_frozen.csv")

    with pytest.raises(ValueError):
        run_live_or_frozen(df, "target", "error", "love")
