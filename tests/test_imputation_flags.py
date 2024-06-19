from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.imputation_flags import create_impute_flags, generate_imputation_marker


@pytest.fixture(scope="class")
def imputation_flag_test_data():
    return load_and_format(Path("tests") / "imputation_flag_data.csv")


class TestImputationFlags:
    def test_create_impute_flags(self, imputation_flag_test_data):
        df_expected_output = imputation_flag_test_data.copy()
        df_expected_output.drop(["imputation_marker"], axis=1, inplace=True)
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
                "f_predictive_auxiliary",
            ]
        ]
        df_output = create_impute_flags(
            df=df_input,
            target="target_variable",
            reference="reference",
            strata="strata",
            auxiliary="auxiliary",
            predictive_auxiliary="f_predictive_auxiliary",
        )

        df_expected_output.drop(["f_predictive_auxiliary"], axis=1, inplace=True)

        assert_frame_equal(df_output, df_expected_output)

    def test_imputation_marker(self, imputation_flag_test_data):
        df_expected_output = imputation_flag_test_data.copy()
        df_input = imputation_flag_test_data.copy()
        df_input.drop("imputation_marker", axis=1, inplace=True)
        df_output = generate_imputation_marker(df_input)
        assert_frame_equal(df_output, df_expected_output)
