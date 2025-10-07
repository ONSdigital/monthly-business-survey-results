import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.stage_dataframe import (
    drop_derived_questions,
    exclude_from_results,
)


@pytest.fixture(scope="class")
def data_dir(staging_data_dir):
    return staging_data_dir / "stage_dataframe"


def test_drop_derived_questions(data_dir):
    df_input = pd.read_csv(data_dir / "drop_derived_questions_input.csv")

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
    expected_output = pd.read_csv(data_dir / "drop_derived_questions_output.csv")

    assert_frame_equal(actual_output, expected_output)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/stage_dataframe")


@pytest.fixture(scope="class")
def responses(filepath):
    return pd.read_csv(filepath / "exclude_from_results_responses.csv", index_col=False)


@pytest.fixture(scope="class")
def contributors(filepath):
    return pd.read_csv(
        filepath / "exclude_from_results_contributors.csv", index_col=False
    )


@pytest.fixture(scope="class")
def expected_output(filepath):
    return pd.read_csv(filepath / "exclude_from_results_output.csv", index_col=False)


@pytest.fixture(scope="class")
def expected_output_csv(filepath):
    return pd.read_csv(
        filepath / "exclude_from_results_output_csv.csv", index_col=False
    )


def test_exclude_from_results_csv(responses, contributors, expected_output_csv):

    actual_output, excluded = exclude_from_results(
        responses=responses,
        contributors=contributors,
        non_response_statuses=["Excluded from Results", "Form sent out"],
        reference="reference",
        period="period",
        status="status",
        target="adjustedresponse",
        question_no="question_no",
    )

    print(list(excluded))

    print(expected_output_csv)

    assert_frame_equal(excluded, expected_output_csv)


@patch("pandas.DataFrame.to_csv")  # mock pandas export to csv function
def test_warning_and_csv(mock_to_csv, caplog, responses, contributors):
    with caplog.at_level(logging.INFO):
        exclude_from_results(
            responses=responses,
            contributors=contributors,
            non_response_statuses=["Excluded from Results", "Form sent out"],
            reference="reference",
            period="period",
            status="status",
            target="adjustedresponse",
            question_no="question_no",
        )

        assert """9 rows have been dropped from responses,""" in caplog.text


@patch("pandas.DataFrame.to_csv")  # mock pandas export to csv function
def test_exclude_from_results(mock_to_csv, responses, contributors, expected_output):

    actual_output, excluded = exclude_from_results(
        responses=responses,
        contributors=contributors,
        non_response_statuses=["Excluded from Results", "Form sent out"],
        reference="reference",
        period="period",
        status="status",
        target="adjustedresponse",
        question_no="question_no",
    )

    assert_frame_equal(actual_output, expected_output)
