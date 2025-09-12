import logging
from pathlib import Path

import pandas as pd
import pytest

from mbs_results.staging.validate_snapshot import validate_snapshot


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


class TestValidateSnapshot:
    def test_validate_snapshot(
        self,
        caplog,
        contributors,
        responses_input,
    ):
        with caplog.at_level(logging.WARN):
            validate_snapshot(
                responses=responses_input,
                contributors=contributors,
                status="status",
                reference="reference",
                period="period",
                non_response_statuses=["Form sent out", "Excluded from results"],
            )

            assert (
                """There are 2 period and
            reference groupings that are listed as non-response statuses in contributors
            but are present in responses."""
                in caplog.text
            )
