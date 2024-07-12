from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.pre_processing_estimation import collate_estimation_data


@pytest.fixture(scope="class")
def filepath():
    return Path("tests") / "data" / "pre_processing_estimation"


@pytest.fixture(scope="class")
def collate_estimation_data_expected(filepath):
    return load_and_format(filepath / "collate_estimation_data.csv")


class TestPreProcessingEstimation:
    def test_collate_estimation_data(self, collate_estimation_data_expected):
        expected = collate_estimation_data_expected[
            ["period", "reference", "cell_num", "auxiliary", "sampled"]
        ]
        population_frame = expected.drop(columns=["sampled"])
        sample = population_frame.loc[:1, ["reference", "period"]]

        actual = collate_estimation_data(
            population_frame, sample, "period", "reference", "cell_num", "auxiliary"
        )

        assert_frame_equal(collate_estimation_data_expected, actual, check_dtype=False)
