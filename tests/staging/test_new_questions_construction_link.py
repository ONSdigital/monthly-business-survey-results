from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.stage_dataframe import new_questions_construction_link


@pytest.fixture(scope="class")
def data_dir():
    return Path("tests/data/staging/missing_construction_link")


@pytest.fixture(scope="class")
def missing_construction_link_input(data_dir):
    return pd.read_csv(
        data_dir / "missing_construction_link_inputs.csv", index_col=False
    )


@pytest.fixture(scope="class")
def missing_construction_link_output(data_dir):
    return pd.read_csv(
        data_dir / "missing_construction_link_outputs.csv", index_col=False
    )


class TestMergeDomain:
    def test_new_questions_construction_values(
        self, missing_construction_link_input, missing_construction_link_output
    ):
        expected_output = missing_construction_link_output
        input_data = missing_construction_link_input

        config = {
            "cell_number": "cell_no",
            "period": "period",
            "reference": "reference",
            "question_no": "questioncode",
        }

        actual_output = new_questions_construction_link(input_data, config)
        assert_frame_equal(actual_output, expected_output, check_dtype=False)
