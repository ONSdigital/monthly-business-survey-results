from pathlib import Path

import pandas as pd
import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.pre_processing_estimation import derive_estimation_variables


@pytest.fixture(scope="class")
def filepath():
    return Path("tests") / "data" / "pre_processing_estimation"


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
                "sampled",
            ]
        ]
        population_frame = expected.drop(columns=["sampled"])
        sample = population_frame.loc[:1, ["reference", "period"]]

        calibration_group_map = pd.DataFrame(
            {
                "cell_no": [123456, 234567, 345678],
                "calibration_number": [123456, 123456, 345678],
            }
        )

        actual = derive_estimation_variables(
            population_frame,
            sample,
            calibration_group_map,
            "period",
            "reference",
            "cell_no",
        )

        assert_frame_equal(
            derive_estimation_variables_data, actual, check_dtype=False, check_like=True
        )
