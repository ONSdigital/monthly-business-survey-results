import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.imputation_flags import generate_imputation_marker
from tests.helper_functions import load_and_format

scenario_path_prefix = "tests/data/imputation/imputation_flags/"

scenarios = [
    "imputation_flag_data.csv",
    "imputation_flag_data_manual_construction.csv",
    "imputation_flag_mc_fimc_fir_bir.csv",
]


@pytest.mark.parametrize("file_name", scenarios)
class TestImputationFlags:
    def test_imputation_marker(self, file_name):
        df = load_and_format(scenario_path_prefix + file_name)

        df_expected_output = df.copy()
        df_input = df.copy()

        # target_variable_man might exist or not
        df_input = df_input[
            df_input.columns.intersection(
                set(
                    [
                        "reference",
                        "strata",
                        "period",
                        "target_variable",
                        "auxiliary",
                        "target_variable_man",
                    ]
                )
            )
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
            axis=1,
            errors="ignore",
        )

        df_output.drop(columns=["is_backdata"], inplace=True)

        assert_frame_equal(df_output, df_expected_output)
