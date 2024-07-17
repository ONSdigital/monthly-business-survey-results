from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.aggregate_weights_by_domain import aggregate_weights_by_domain


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/estimation")


@pytest.fixture(scope="class")
def aggregate_weights_by_domain_test_input(filepath):
    return pd.read_csv(
        filepath / "aggregate_weights_by_domain_input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def aggregate_weights_by_domain_test_output(filepath):
    return pd.read_csv(
        filepath / "aggregate_weights_by_domain_output.csv", index_col=False
    )


class TestAggregateWeightsByDomain:
    def test_aggregate_weights_by_domain(
        self,
        aggregate_weights_by_domain_test_input,
        aggregate_weights_by_domain_test_output,
    ):
        expected_output = aggregate_weights_by_domain_test_output

        input_data = aggregate_weights_by_domain_test_input

        actual_output = aggregate_weights_by_domain(
            input_data, "period", "domain", "a_weight", "o_weight", "g_weight", 202401
        )

        assert_frame_equal(actual_output, expected_output)
