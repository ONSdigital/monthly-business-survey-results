from pathlib import Path

import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.apply_imputation_link import (
    create_and_merge_imputation_values,
)
from tests.helper_functions import load_and_format


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/imputation/apply_imputation_link")


class TestApplyImputationLink:
    def test_all_imputation_types(self, filepath):
        loaded_data = load_and_format(filepath / "FIR_BIR_C_FIC.csv")

        input_data = loaded_data.drop(columns=["expected_target"])
        expected_output = loaded_data.drop(columns="target").rename(
            columns={"expected_target": "target"}
        )
        expected_output["target"] = expected_output["target"].astype(float)

        actual_output = create_and_merge_imputation_values(
            input_data,
            "imputation_class",
            "reference",
            "period",
            "imputation_marker",
            "target",
            "cumulative_forward_link",
            "cumulative_backward_link",
            "auxiliary_variable",
            "construction_link",
            imputation_types=("c", "fir", "bir", "fic"),
        )

        actual_output = actual_output.sort_index(axis=1)
        expected_output = expected_output.sort_index(axis=1)
        assert_frame_equal(actual_output, expected_output)

    def test_mc_imputation_types(self, filepath):
        loaded_data = load_and_format(filepath / "MC_FIMC.csv")

        input_data = loaded_data.drop(columns=["expected_target"])
        input_data["man_link"] = 1

        expected_output = loaded_data.drop(columns="target").rename(
            columns={"expected_target": "target"}
        )
        expected_output["target"] = expected_output["target"].astype(float)

        actual_output = create_and_merge_imputation_values(
            input_data,
            "imputation_class",
            "reference",
            "period",
            "imputation_marker",
            "target",
            "cumulative_forward_link",
            "cumulative_backward_link",
            "auxiliary_variable",
            "construction_link",
            imputation_types=("c", "mc", "fir", "bir", "fimc", "fic"),
        )
        actual_output.drop(columns=["man_link"], inplace=True)

        actual_output = actual_output.sort_index(axis=1)
        expected_output = expected_output.sort_index(axis=1)

        assert_frame_equal(actual_output, expected_output)
