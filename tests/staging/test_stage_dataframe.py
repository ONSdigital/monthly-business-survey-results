import logging
import os
import tempfile
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
    with tempfile.TemporaryDirectory() as tmpdirname:
        actual_output = exclude_from_results(
            responses=responses,
            contributors=contributors,
            non_response_statuses=["Excluded from Results", "Form sent out"],
            reference="reference",
            period="period",
            status="status",
            target="adjustedresponse",
            imputation_marker="imputation_marker_adjustedresponse",
            question_no="question_no",
            output_path=tmpdirname,
        )
        actual_output = pd.read_csv(
            os.path.join(tmpdirname, "excluded_from_results.csv")
        )

    assert_frame_equal(actual_output, expected_output_csv)


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
            imputation_marker="imputation_marker_adjustedresponse",
            question_no="question_no",
            output_path="test_outputs/",
        )

        assert """9 rows set to null for target and""" in caplog.text

        mock_to_csv.assert_called_once_with(
            "test_outputs/excluded_from_results.csv", index=False
        )


@patch("pandas.DataFrame.to_csv")  # mock pandas export to csv function
def test_exclude_from_results(mock_to_csv, responses, contributors, expected_output):

    actual_output = exclude_from_results(
        responses=responses,
        contributors=contributors,
        non_response_statuses=["Excluded from Results", "Form sent out"],
        reference="reference",
        period="period",
        status="status",
        target="adjustedresponse",
        imputation_marker="imputation_marker_adjustedresponse",
        question_no="question_no",
        output_path="test_outputs/",
    )
    print(actual_output.columns)
    assert_frame_equal(actual_output, expected_output)
