import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.ratio_of_means import ratio_of_means
from mbs_results.utilities.utils import convert_column_to_datetime
from tests.helper_functions import load_filter

scenario_path_prefix = "tests/data/imputation/ratio_of_means/"

scenarios = [
    "01_C",
    "02_C_FI",
    "03_R_R_FI",
    "04_R_R_FI_FI",
    "05_R_R_FI_FI_FI_year_span",
    "06_BI_BI_R",
    "07_BI_BI_R_FI_FI_R_FI",
    "08_R_R_R",
    "09_R_NS_C",
    "10_C_FI_NS_R",
    "11_R_R_FI-BI_R_R",
    "12_C_FI_FI_FI_FI",
    "13_R_FI_FI_NS_BI_BI_R",
    "14_C_FI_FI_NS_BI_BI_R",
    "15_BI_BI_R_NS_R_FI_FI",
    "16_BI_BI_R_NS_C_FI_FI",
    "17_NS_R_FI_NS",
    "18_NS_BI_R_NS",
    "19_link_columns",
    "20_mixed_data",
    "21_class_change_R_C_FI",
    "22_class_change_C_BI_R",
    "23_class_change_C_C_FI",
    "24_class_change_R_BI_R",
    "25_class_change_C_FI_FI",
    "26_C_FI_FI_NS_BI_BI_R_filtered",
    "27_BI_BI_R_NS_R_FI_FI_filtered",
    "28_link_columns_filtered",
    "29_mixed_data_filtered",
    "30_class_change_C_C_FI_filtered",
    "31_no_response",
    "32_divide_by_zero",
    "33_multi_variable_C_BI_R",
    "34_multi_variable_C_BI_R_filtered",
    "35_BI_BI_R_FI_FI_R_FI_alternating_filtered",
    "rom_test_data_case_mc_1",
    "rom_test_data_case_mc_2",
    "rom_test_data_case_mc_3",
    "rom_test_data_case_mc_4",
    "rom_test_data_case_mc_5",
    "rom_test_data_case_mc_6",
    "rom_test_data_case_mc_7",
    "rom_test_data_case_mc_8",
    "rom_test_data_case_mc_9",
    "rom_test_data_case_mc_10",
]


@pytest.mark.parametrize("base_file_name", scenarios)
class TestRatioOfMeans:
    def test_ratio_of_means(self, base_file_name):

        input_data = pd.read_csv(scenario_path_prefix + base_file_name + "_input.csv")
        expected_output = pd.read_csv(
            scenario_path_prefix + base_file_name + "_output.csv"
        )

        filter_df = load_filter(
            scenario_path_prefix + "ratio_of_means_filters/" + base_file_name + ".csv"
        )

        # Can't use load_format helper, test cases have date instead of period

        input_data["date"] = pd.to_datetime(input_data["date"], format="%Y%m")
        expected_output["date"] = pd.to_datetime(expected_output["date"], format="%Y%m")

        if base_file_name in ["19_link_columns", "28_link_columns_filtered"]:
            actual_output = ratio_of_means(
                input_data,
                target="question",
                period="date",
                reference="identifier",
                strata="group",
                auxiliary="other",
                question_no="questioncode",
                filters=filter_df,
                imputation_links={
                    "forward": "f_link_question",
                    "backward": "b_link_question",
                    "construction": "construction_link",
                },
                current_period=202001,
                revision_period=10,
            )
        else:
            actual_output = ratio_of_means(
                input_data,
                target="question",
                period="date",
                reference="identifier",
                strata="group",
                auxiliary="other",
                question_no="questioncode",
                filters=filter_df,
                current_period=202001,
                revision_period=10,
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
                "marker": "imputation_flags_question",
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


@pytest.mark.parametrize("mc_base_file_name", scenarios[-10:])
class TestRatioOfMeansManConstruction:
    def test_manual_construction_input(self, mc_base_file_name):
        df = pd.read_csv(scenario_path_prefix + mc_base_file_name + "_input.csv")
        expected_output = pd.read_csv(
            scenario_path_prefix + mc_base_file_name + "_output.csv"
        )

        manual_constructions = df.copy()[
            ["identifier", "date", "question_man", "question_no"]
        ]
        manual_constructions.rename(columns={"question_man": "question"}, inplace=True)

        df.drop(columns=["question_man"], inplace=True)
        input_data = df
        input_data["date"] = convert_column_to_datetime(input_data["date"])

        manual_constructions["date"] = convert_column_to_datetime(
            manual_constructions["date"]
        )

        actual_output = ratio_of_means(
            input_data,
            target="question",
            period="date",
            reference="identifier",
            strata="group",
            auxiliary="other",
            question_no="question_no",
            manual_constructions=manual_constructions,
            current_period=202001,
            revision_period=10,
        )

        expected_output["date"] = convert_column_to_datetime(expected_output["date"])
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
                "marker": "imputation_flags_question",
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
        expected_output = expected_output[actual_output.columns]

        actual_output = actual_output.sort_values(by=["identifier", "date"])
        expected_output = expected_output.sort_values(by=["identifier", "date"])

        actual_output = actual_output.reset_index(drop=True)
        expected_output = expected_output.reset_index(drop=True)

        expected_output["imputation_flags_question"] = expected_output[
            "imputation_flags_question"
        ].str.lower()
        expected_output = expected_output.replace({"bi": "bir"})

        expected_output["f_match_question_pair_count"] = expected_output[
            "f_match_question_pair_count"
        ].astype(float)
        expected_output["b_match_question_pair_count"] = expected_output[
            "b_match_question_pair_count"
        ].astype(float)
        expected_output["flag_match_pair_count"] = expected_output[
            "flag_match_pair_count"
        ].astype(float)

        assert_frame_equal(expected_output, actual_output)
