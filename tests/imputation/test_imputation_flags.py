from pathlib import Path

import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.imputation_flags import generate_imputation_marker
from tests.helper_functions import load_and_format


@pytest.fixture(scope="class")
def imputation_flag_test_data():
    return load_and_format(
        Path("tests/data/imputation") / "imputation_flags/imputation_flag_data.csv"
    )


@pytest.fixture(scope="class")
def imputation_flag_test_data_manual():
    return load_and_format(
        Path("tests/data/imputation")
        / "imputation_flags/imputation_flag_data_manual_construction.csv"
    )


class TestImputationFlags:
    def test_imputation_marker(self, imputation_flag_test_data):
        df_expected_output = imputation_flag_test_data.copy()
        df_input = imputation_flag_test_data.copy()
        df_input.drop("imputation_flags_target_variable", axis=1, inplace=True)
        df_input = df_input[
            [
                "reference",
                "strata",
                "period",
                "target_variable",
                "auxiliary",
            ]
        ]
        df_output = generate_imputation_marker(
            df=df_input,
            target="target_variable",
            period="period",
            reference="reference",
            strata="strata",
            auxiliary="auxiliary",
            predictive_auxiliary="f_match_auxiliary",
            back_data_period=111,
        )
        df_expected_output.drop(
            columns=[
                "f_match_target_variable",
                "b_match_target_variable",
                "f_match_auxiliary",
            ],
            inplace=True,
        )
        df_output.drop(columns=["is_backdata"], inplace=True)

        assert_frame_equal(df_output, df_expected_output)

    def test_imputation_marker_manual_construction(
        self, imputation_flag_test_data_manual
    ):
        df_expected_output = imputation_flag_test_data_manual.copy()
        df_input = imputation_flag_test_data_manual.copy()
        df_input.drop("imputation_flags_target_variable", axis=1, inplace=True)
        df_input = df_input[
            [
                "reference",
                "strata",
                "period",
                "target_variable",
                "auxiliary",
                "target_variable_man",
            ]
        ]

        df_output = generate_imputation_marker(
            df=df_input,
            target="target_variable",
            period="period",
            reference="reference",
            strata="strata",
            auxiliary="auxiliary",
            back_data_period=111,
        )
        df_expected_output.drop(
            columns=[
                "f_match_target_variable",
                "b_match_target_variable",
                "f_match_auxiliary",
            ],
            inplace=True,
        )
        df_output.drop(columns=["is_backdata"], inplace=True)

        assert_frame_equal(df_output, df_expected_output)
