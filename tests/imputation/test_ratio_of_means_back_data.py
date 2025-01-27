import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.ratio_of_means import ratio_of_means

scenario_path_prefix = "tests/data/imputation/back_data_testing/"

scenarios = [
    "R_FIR_FIR",
    "FIR_FIR_FIR",
    "C_FIC_FIC",
    "FIC_FIC_FIC",
    "MC_FIMC_FIMC",
    "FIMC_FIMC_FIMC",
]


pytestmark = pytest.mark.parametrize("base_file_name", scenarios)


class TestRatioOfMeans:
    def test_ratio_of_means_back_data(self, base_file_name):

        input_data = pd.read_csv(scenario_path_prefix + base_file_name + "_input.csv")
        expected_output = pd.read_csv(
            scenario_path_prefix + base_file_name + "_output.csv"
        )

        # Can't use load_format helper, test cases have date instead of period

        input_data["date"] = pd.to_datetime(input_data["date"], format="%Y%m")
        expected_output["date"] = pd.to_datetime(expected_output["date"], format="%Y%m")

        actual_output = ratio_of_means(
            input_data,
            target="question",
            period="date",
            reference="identifier",
            strata="group",
            auxiliary="other",
            question_no="questioncode",
            current_period=202003,
            revision_period=2,
        )

        actual_output = actual_output.rename(
            columns={
                "default_link_b_match_question": "default_backward",
                "default_link_f_match_question": "default_forward",
                "default_link_flag_construction_matches": "default_construction",
                "flag_construction_matches_pair_count": "flag_match_pair_count",
            }
        )

        actual_output = actual_output.drop(columns=["other"])

        # if stays like this we need a function to load expected data
        expected_output = expected_output.rename(
            columns={
                "output": "question",
                "forward": "f_link_question",
                "backward": "b_link_question",
                "construction": "construction_link",
                "count_forward": "f_match_question_pair_count",
                "count_backward": "b_match_question_pair_count",
                "count_construction": "flag_match_pair_count",
            }
        )

        actual_output.drop(columns=["question_man"], errors="ignore", inplace=True)
        # Temp work around to drop mc column until its fully integrated
        actual_output.drop(
            columns=[
                "b_match_filtered_question",
                "b_predictive_filtered_question",
                "b_link_filtered_question",
                "f_predictive_filtered_question",
                "f_link_filtered_question",
                "filtered_question",
                "cumulative_b_link_filtered_question",
                "cumulative_f_link_filtered_question",
            ],
            errors="ignore",
            inplace=True,
        )
        actual_output.drop(
            columns=["forward", "backward", "construction"],
            errors="ignore",
            inplace=True,
        )
        actual_output.drop(
            columns=["is_backdata", "backdata_flags_question", "backdata_question"],
            errors="ignore",
            inplace=True,
        )

        expected_output = expected_output[actual_output.columns]

        actual_output = actual_output.sort_values(by=["identifier", "date"])
        expected_output = expected_output.sort_values(by=["identifier", "date"])

        actual_output = actual_output.reset_index(drop=True)
        expected_output = expected_output.reset_index(drop=True)

        expected_output["imputation_flags_question"] = expected_output[
            "imputation_flags_question"
        ].str.lower()
        expected_output = expected_output.replace({"bi": "bir"})

        assert_frame_equal(actual_output, expected_output, check_dtype=False)
