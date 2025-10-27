import logging
from pathlib import Path

import pandas as pd
import pytest

from mbs_results.staging.validate_snapshot import (
    check_for_null_target,
    non_response_in_responses,
)


@pytest.fixture(scope="class")
def check_for_null_target_responses(filepath):
    return pd.read_csv(
        filepath / "check_for_null_target_responses.csv", index_col=False
    )


@pytest.fixture(scope="class")
def config():
    return {
        "filter_out_questions": [11, 12],
        "debug_mode": False,
        "status": "status",
    }


@pytest.fixture(scope="class")
def expected_check_for_null_target(filepath):
    return pd.read_csv(
        filepath.parent
        / "validate_snapshot"
        / "check_for_null_target_expected_output.csv",
        index_col=False,
    )


class TestCheckForNullTarget:
    def test_check_for_null_target_output(
        self, check_for_null_target_responses, config, expected_check_for_null_target
    ):
        result_df = check_for_null_target(
            config=config,
            responses=check_for_null_target_responses,
            target="target",
            question_no="questioncode",
        )
        # Only compare columns present in expected output
        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True)[expected_check_for_null_target.columns],
            expected_check_for_null_target,
        )

    def test_check_for_null_target_nulls(
        self, caplog, check_for_null_target_responses, config
    ):
        with caplog.at_level(logging.WARN):
            check_for_null_target(
                config=config,
                responses=check_for_null_target_responses,
                target="target",
                question_no="questioncode",
            )
        assert "There are 4 rows with nulls" in caplog.text

    def test_check_for_null_target_empty_strings(
        self, caplog, check_for_null_target_responses, config
    ):
        responses = check_for_null_target_responses.copy()
        responses["target"] = responses["target"].fillna("")
        with caplog.at_level(logging.WARN):
            check_for_null_target(
                config=config,
                responses=responses,
                target="target",
                question_no="questioncode",
            )
        assert "There are 6 rows with empty strings" in caplog.text


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/validate_snapshot")


@pytest.fixture(scope="class")
def contributors(filepath):
    return pd.read_csv(
        filepath / "non_response_in_response_contributors.csv", index_col=False
    )


@pytest.fixture(scope="class")
def responses_input(filepath):
    return pd.read_csv(
        filepath / "non_response_in_response_responses.csv", index_col=False
    )


@pytest.fixture(scope="class")
def expected_non_response_in_responses(filepath):
    return pd.read_csv(
        filepath.parent
        / "validate_snapshot"
        / "non_response_in_responses_expected_output.csv",
        index_col=False,
    )


class TestNonResponseInResponses:
    def test_non_response_in_responses_output(
        self,
        contributors,
        responses_input,
        expected_non_response_in_responses,
        config,
    ):
        result_df = non_response_in_responses(
            responses=responses_input,
            contributors=contributors,
            status="status",
            reference="reference",
            period="period",
            non_response_statuses=["Form sent out", "Excluded from results"],
            config=config,
        )
        result_df = result_df.reset_index()

        pd.testing.assert_frame_equal(
            result_df,
            expected_non_response_in_responses,
        )

    def test_non_response_in_responses(
        self,
        caplog,
        contributors,
        responses_input,
        config,
    ):
        with caplog.at_level(logging.WARN):
            non_response_in_responses(
                responses=responses_input,
                contributors=contributors,
                status="status",
                reference="reference",
                period="period",
                non_response_statuses=["Form sent out", "Excluded from results"],
                config=config,
            )
            assert (
                """There are 2 period and
            reference groupings that are listed as non-response statuses in contributors
            but are present in responses."""
                in caplog.text
            )
