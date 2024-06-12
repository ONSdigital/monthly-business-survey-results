from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.ratio_of_means import ratio_of_means


scenario_path_prefix = Path("tests", "data", "ratio_of_means")

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
    "13_R_FI_FI_NS_BI_BI_R",  # bug
    "14_C_FI_FI_NS_BI_BI_R",  # bug
    "15_BI_BI_R_NS_R_FI_FI",  # bug
    "16_BI_BI_R_NS_C_FI_FI",  # bug
    "17_NS_R_FI_NS",
    "18_NS_BI_R_NS",
    "19_link_columns",  # bug
    "20_mixed_data",  # bug
    "21_class_change_R_C_FI",
    "22_class_change_C_BI_R",
    "23_class_change_C_C_FI",
    "24_class_change_R_BI_R",
    "25_class_change_C_FI_FI",
    # '26_C_FI_FI_NS_BI_BI_R_filtered', #not yet implemented
    # '27_BI_BI_R_NS_R_FI_FI_filtered', #not yet implemented
    # '28_link_columns_filtered', #not yet implemented
    # '29_mixed_data_filtered', #not yet implemented
    # '30_class_change_C_C_FI_filtered',  #not yet implemented
    "31_no_response",  # bug
    # '32_divide_by_zero', #not yet implemented
    "33_multi_variable_C_BI_R",
    # '34_multi_variable_C_BI_R_filtered',  #not yet implemented
    # '35_BI_BI_R_FI_FI_R_FI_alternating_filtered' #not yet implemented
]


pytestmark = pytest.mark.parametrize("base_file_name", scenarios)


class TestRatioOfMeans:
    def test_ratio_of_means(self, base_file_name):

        input_data = pd.read_csv(Path(scenario_path_prefix,base_file_name + "_input.csv"))
        expected_output = pd.read_csv(Path(scenario_path_prefix,base_file_name + "_output.csv"))

        # not yet implemented remove this when defaults are ready
        expected_output = expected_output.drop(
            columns=["default_forward", "default_backward", "default_construction"]
        )

        # remove this if no datetime implemented , now is needed
        input_data["date"] = pd.to_datetime(input_data["date"], format="%Y%m")
        expected_output["date"] = pd.to_datetime(expected_output["date"], format="%Y%m")

        actual_output = ratio_of_means(
            input_data,
            target="question",
            period="date",
            reference="identifier",
            strata="group",
            auxiliary="other",
        )

        # imputed_value is in a seperate column, remove this if otherwise
        actual_output["question"] = (
            actual_output[["question", "imputed_value"]].agg(sum, axis=1).round(2)
        )

        actual_output = actual_output.drop(columns=["imputed_value"])

        # if stays like this we need a function to load expected data
        expected_output = expected_output.rename(
            columns={
                "output": "question",
                "marker": "imputation_marker",
                "forward": "f_link_question",
                "backward": "b_link_question",
                "construction": "construction_link",
                "count_forward": "f_matched_pair_count",
                "count_backward": "b_matched_pair_count",
                "count_construction": "flag_matched_pair_count",
            }
        )

        expected_output = expected_output[actual_output.columns]

        assert_frame_equal(actual_output, expected_output)
