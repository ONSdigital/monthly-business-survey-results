from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from mbs_results.data_cleaning import enforce_datatypes, validate_config_datatype_input


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
    test_setup_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {"period": "date", "strata": "str"},
        "contributors_keep_cols": {"reference": "int", "target_variable": "float"},
        "temporarily_remove_cols": [],
    }
    validate_config_datatype_input(**test_setup_config)
    actual_output = enforce_datatypes(df_subset, **test_setup_config)

    assert_frame_equal(actual_output, expected_output)
