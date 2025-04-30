from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.stage_dataframe import drop_derived_questions


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/stage_dataframe")


def test_drop_derived_questions(filepath):
    df_input = pd.read_csv(filepath / "drop_derived_questions_input.csv")

    test_config = {
        "form_to_derived_map": {
            13: [40],
            14: [40],
            15: [46],
            16: [42],
        }
    }

    actual_output = drop_derived_questions(
        df_input, "question_no", "form_type", test_config["form_to_derived_map"]
    )
    expected_output = pd.read_csv(filepath / "drop_derived_questions_output.csv")

    assert_frame_equal(actual_output, expected_output)


def test_drop_derived_questions_multiple(filepath):
    df_input = pd.read_csv(filepath / "drop_derived_questions_multi_input.csv")

    test_config = {"form_to_derived_map": {13: [40, 46, 42, 40]}}

    actual_output = drop_derived_questions(
        df_input, "question_no", "form_type", test_config["form_to_derived_map"]
    )
    expected_output = pd.read_csv(filepath / "drop_derived_questions_multi_output.csv")

    assert_frame_equal(actual_output, expected_output)
