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
def input(filepath):
    return pd.read_csv(filepath / "exclude_from_results_input.csv", index_col=False)


@pytest.fixture(scope="class")
def expected_output(filepath):
    return pd.read_csv(filepath / "exclude_from_results_output.csv", index_col=False)


@pytest.fixture(scope="class")
def expected_csv(filepath):
    return pd.read_csv(
        filepath / "exclude_from_results_output_csv.csv", index_col=False
    )


class TestExcludeFromResults:
    @patch("pandas.DataFrame.to_csv")  # mock pandas export to csv function
    def test_warning_and_csv(
        self,
        mock_to_csv,
        caplog,
        input,
    ):
        with caplog.at_level(logging.INFO):
            exclude_from_results(
                df=input,
                non_response_statuses=["Excluded from Results", "Form sent out"],
                reference="reference",
                period="period",
                status="status",
                target="adjustedresponse",
                imputation_marker="imputation_marker_adjustedresponse",
                question_no="question_no",
            )

            assert (
                """9 rows set to null for target and imputation_marker""" in caplog.text
            )

            mock_to_csv.assert_called_once_with(
                "excluded_from_results.csv", index=False
            )

    @patch("pandas.DataFrame.to_csv")  # mock pandas export to csv function
    def test_exclude_from_results(self, mock_to_csv, input, expected_output):

        actual_output = exclude_from_results(
            df=input,
            non_response_statuses=["Excluded from Results", "Form sent out"],
            reference="reference",
            period="period",
            status="status",
            target="adjustedresponse",
            imputation_marker="imputation_marker_adjustedresponse",
            question_no="question_no",
        )
        mock_to_csv.assert_called_once_with("excluded_from_results.csv", index=False)
        assert_frame_equal(actual_output, expected_output)
