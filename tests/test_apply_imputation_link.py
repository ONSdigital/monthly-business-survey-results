from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.imputation.apply_imputation_link import create_and_merge_imputation_values


@pytest.fixture(scope="class")
def fir_bir_c_fic_test_data():
    return load_and_format(
        Path("tests") / "data" / "apply_imputation_link" / "FIR_BIR_C_FIC.csv"
    )


@pytest.fixture(scope="class")
def mc_fimc_test_data():
    return load_and_format(
        Path("tests") / "data" / "apply_imputation_link" / "MC_FIMC.csv"
    )


class TestApplyImputationLink:
    def test_all_imputation_types(self, fir_bir_c_fic_test_data):
        expected_output = fir_bir_c_fic_test_data

        input_data = expected_output.drop(columns=["imputed_value"])
        actual_output = create_and_merge_imputation_values(
            input_data,
            "imputation_class",
            "reference",
            "period",
            "imputation_marker",
            "imputed_value",
            "target",
            "cumulative_forward_link",
            "cumulative_backward_link",
            "auxiliary_variable",
            "construction_link",
            imputation_types=("c", "fir", "bir", "fic"),
        )

        assert_frame_equal(actual_output, expected_output)

    def test_mc_imputation_types(self, mc_fimc_test_data):
        expected_output = mc_fimc_test_data

        input_data = expected_output.drop(columns=["imputed_value"])
        input_data["man_link"] = 1
        actual_output = create_and_merge_imputation_values(
            input_data,
            "imputation_class",
            "reference",
            "period",
            "imputation_marker",
            "imputed_value",
            "target",
            "cumulative_forward_link",
            "cumulative_backward_link",
            "auxiliary_variable",
            "construction_link",
            imputation_types=("c", "mc", "fir", "bir", "fimc", "fic"),
        )
        actual_output.drop(columns=["man_link"], inplace=True)

        assert_frame_equal(actual_output, expected_output)
