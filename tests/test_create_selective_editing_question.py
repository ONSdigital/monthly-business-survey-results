from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.selective_editing_question import create_selective_editing_question


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/estimation")


@pytest.fixture(scope="class")
def create_selective_editing_question_input(filepath):
    return pd.read_csv(
        filepath / "create_selective_editing_question_input.csv", index_col=False
    )


class TestCreateSelectiveEditingQuestion:
    def test_create_selective_editing_question(
        self,
        create_selective_editing_question_input,
    ):
        expected_output = create_selective_editing_question_input[
            create_selective_editing_question_input["standardising_factor"].notna()
        ]
        expected_output = expected_output[
            [
                "period",
                "reference",
                "question_code",
                "standardising_factor",
                "predicted_value",
                "imputation_marker",
                "auxiliary_value",
            ]
        ]
        expected_output = expected_output.reset_index(drop=True)

        input_data = create_selective_editing_question_input.drop(
            columns="standardising_factor"
        )

        actual_output = create_selective_editing_question(
            input_data,
            "reference",
            "period",
            "domain",
            "question_code",
            "predicted_value",
            "imputation_marker",
            "a_weight",
            "o_weight",
            "g_weight",
            "auxiliary_value",
            202401,
        )

        assert_frame_equal(actual_output, expected_output)
