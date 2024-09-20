from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.create_missing_questions import create_missing_questions


@pytest.fixture(scope="class")
def filepath():
    return Path("tests")


@pytest.fixture(scope="class")
def create_missing_questions_input_con(filepath):
    return pd.read_csv(
        filepath / "data" / "create_missing_questions_contributors.csv", index_col=False
    )


@pytest.fixture(scope="class")
def create_missing_questions_input_res(filepath):
    return pd.read_csv(
        filepath / "data" / "create_missing_questions_responses.csv", index_col=False
    )


@pytest.fixture(scope="class")
def create_missing_questions_output(filepath):
    return pd.read_csv(
        filepath / "data" / "create_missing_questions_output.csv", index_col=False
    )


class TestCreateMissingQuestions:
    def test_create_missing_questions(
        self,
        create_missing_questions_input_con,
        create_missing_questions_input_res,
        create_missing_questions_output,
    ):
        mapper = {9: [40, 49], 10: [110]}

        actual_output = create_missing_questions(
            create_missing_questions_input_con,
            create_missing_questions_input_res,
            "reference",
            "period",
            "survey",
            "questioncode",
            mapper,
        )
        expected_output = create_missing_questions_output

        assert_frame_equal(actual_output, expected_output)
