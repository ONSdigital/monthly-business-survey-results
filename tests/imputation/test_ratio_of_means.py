import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.ratio_of_means import ratio_of_means
from tests.helper_functions import load_and_format, load_filter

scenario_path_prefix = "tests/data/imputation/ratio_of_means/"

base_scenarios = [
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
    "20_mixed_data",
    "21_class_change_R_C_FI",
    "22_class_change_C_BI_R",
    "23_class_change_C_C_FI",
    "24_class_change_R_BI_R",
    "25_class_change_C_FI_FI",
    "26_C_FI_FI_NS_BI_BI_R_filtered",
    "27_BI_BI_R_NS_R_FI_FI_filtered",
    "29_mixed_data_filtered",
    "30_class_change_C_C_FI_filtered",
    "31_no_response",
    "32_divide_by_zero",
    "33_multi_variable_C_BI_R",
    "34_multi_variable_C_BI_R_filtered",
    "35_BI_BI_R_FI_FI_R_FI_alternating_filtered",
]

pre_defined_link_scenarios = ["19_link_columns", "28_link_columns_filtered"]

manual_constructions_scenarios = [
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


@pytest.mark.parametrize("base_file_name", base_scenarios)
def test_ratio_of_means(base_file_name):

    input_data = load_and_format(scenario_path_prefix + base_file_name + "_input.csv")

    expected_output = load_and_format(
        scenario_path_prefix + base_file_name + "_output.csv"
    )

    filter_df = load_filter(
        scenario_path_prefix + "ratio_of_means_filters/" + base_file_name + ".csv"
    )

    # add imputation_flag onto data
    input_data["imputation_flags_question"] = pd.Series(dtype="str")

    actual_output = ratio_of_means(
        input_data.copy(),
        target="question",
        period="period",
        reference="identifier",
        strata="group",
        auxiliary="other",
        question_no="questioncode",
        filters=filter_df,
        current_period=202001,
        revision_window=10,
    )

    assert_frame_equal(actual_output, expected_output, check_dtype=False)


@pytest.mark.parametrize("link_scenario", pre_defined_link_scenarios)
def test_ratio_of_means_with_links(link_scenario):

    input_data = load_and_format(scenario_path_prefix + link_scenario + "_input.csv")

    expected_output = load_and_format(
        scenario_path_prefix + link_scenario + "_output.csv"
    )

    filter_df = load_filter(
        scenario_path_prefix + "ratio_of_means_filters/" + link_scenario + ".csv"
    )

    # add imputation_flag onto data
    input_data["imputation_flags_question"] = pd.Series(dtype="str")

    actual_output = ratio_of_means(
        input_data,
        target="question",
        period="period",
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
        revision_window=10,
    )

    assert_frame_equal(actual_output, expected_output, check_dtype=False)


@pytest.mark.parametrize("mc_base_file_name", manual_constructions_scenarios)
def test_ratio_of_means_with_manual_constructions(mc_base_file_name):

    df = load_and_format(scenario_path_prefix + mc_base_file_name + "_input.csv")

    df["imputation_flags_question"] = pd.Series(dtype="str")

    expected_output = load_and_format(
        scenario_path_prefix + mc_base_file_name + "_output.csv"
    )

    manual_constructions = df.copy()[
        ["identifier", "period", "question_man", "question_no"]
    ]
    manual_constructions.rename(columns={"question_man": "question"}, inplace=True)

    df.drop(columns=["question_man"], inplace=True)

    actual_output = ratio_of_means(
        df,
        target="question",
        period="period",
        reference="identifier",
        strata="group",
        auxiliary="other",
        question_no="question_no",
        manual_constructions=manual_constructions,
        current_period=202001,
        revision_window=10,
    )

    assert_frame_equal(actual_output, expected_output, check_dtype=False)
