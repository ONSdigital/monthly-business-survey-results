from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from src.imputation_flags import create_impute_flags, generate_imputation_flag_string


@pytest.fixture(scope="class")
def imputation_flag_test_data():
    return load_and_format(Path("tests") / "imputation_flag_data.csv")


class TestImputationFlags:
    def test_create_impute_flags(self, imputation_flag_test_data):
        df_expected_output = imputation_flag_test_data.copy()
        df_expected_output.drop(["imputation_flag"], axis=1, inplace=True)
        df_input = df_expected_output.copy()
        df_input = df_input[
            [
                "reference",
                "strata",
                "period",
                "target_variable",
                "auxiliary",
                "f_predictive_target_variable",
                "b_predictive_target_variable",
            ]
        ]
        df_output = create_impute_flags(
            df=df_input,
            target="target_variable",
            reference="reference",
            strata="strata",
            auxiliary="auxiliary",
        )
        assert_frame_equal(df_output, df_expected_output)

    def test_imputation_flag_strings(self, imputation_flag_test_data):
        df_expected_output = imputation_flag_test_data.copy()
        df_input = imputation_flag_test_data.copy()
        df_input.drop("imputation_flag", axis=1, inplace=True)
        df_output = generate_imputation_flag_string(df_input)
        assert_frame_equal(df_output, df_expected_output)
