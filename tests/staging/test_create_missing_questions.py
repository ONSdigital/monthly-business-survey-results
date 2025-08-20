import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.create_missing_questions import create_missing_questions


@pytest.fixture(scope="class")
def data_dir(staging_data_dir):
    return staging_data_dir / "create_missing_questions"


@pytest.fixture(scope="class")
def create_missing_questions_input_con(data_dir):
    return pd.read_csv(
        data_dir / "create_missing_questions_contributors.csv", index_col=False
    )


@pytest.fixture(scope="class")
def create_missing_questions_input_res(data_dir):
    return pd.read_csv(
        data_dir / "create_missing_questions_responses.csv", index_col=False
    )


@pytest.fixture(scope="class")
def create_missing_questions_output(data_dir):
    return pd.read_csv(
        data_dir / "create_missing_questions_output.csv", index_col=False
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
        actual_output.sort_values(
            by=["reference", "period", "survey", "questioncode"], inplace=True
        )
        actual_output["questioncode"] = actual_output["questioncode"].astype(int)
        actual_output.reset_index(drop=True, inplace=True)

        expected_output = create_missing_questions_output

        assert_frame_equal(actual_output, expected_output)
