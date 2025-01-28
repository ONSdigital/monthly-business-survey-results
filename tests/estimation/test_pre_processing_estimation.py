from pathlib import Path

import pytest
from pandas.testing import assert_frame_equal

from mbs_results.estimation.pre_processing_estimation import derive_estimation_variables
from tests.helper_functions import load_and_format


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/estimation/pre_processing_estimation")


@pytest.fixture(scope="class")
def derive_estimation_variables_data(filepath):
    return load_and_format(filepath / "derive_estimation_variables.csv")


class TestPreProcessingEstimation:
    def test_derive_estimation_variables(self, derive_estimation_variables_data):
        expected = derive_estimation_variables_data[
            [
                "period",
                "reference",
                "cell_no",
                "calibration_group",
                "auxiliary",
                "is_sampled",
                "ni_gb_cell_number"
            ]
        ]

        original = expected.drop(columns="cell_no").rename(columns={"ni_gb_cell_number": "cell_no"})
        original = original[
            [
                "period",
                "reference",
                "cell_no",
                "calibration_group",
                "auxiliary",
                "is_sampled"
            ]
        ]

        population_frame = original.drop(columns=["calibration_group", "is_sampled"])
        sample = population_frame.loc[:1, ["reference", "period"]]

        calibration_group_map = expected[["cell_no", "calibration_group"]]

        actual = derive_estimation_variables(
            population_frame,
            sample,
            calibration_group_map,
            "period",
            "reference",
            "cell_no",
        )

        assert_frame_equal(expected, actual, check_dtype=False, check_like=True)
