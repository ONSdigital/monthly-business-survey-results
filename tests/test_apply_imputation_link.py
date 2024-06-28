from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.apply_imputation_link import create_and_merge_imputation_values


@pytest.fixture(scope="class")
def fir_bir_c_fic_test_data():
    return load_and_format(
        Path("tests") / "data" / "apply_imputation_link" / "FIR_BIR_C_FIC.csv"
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
