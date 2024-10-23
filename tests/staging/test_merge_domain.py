from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.merge_domain import merge_domain


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/merge_domain")


@pytest.fixture(scope="class")
def merged_domain_test_data(filepath):
    return pd.read_csv(filepath / "merge_domain.csv", index_col=False)


@pytest.fixture(scope="class")
def domain_mapping_test_data(filepath):
    return pd.read_csv(filepath / "domain_mapping.csv", index_col=False)


class TestMergeDomain:
    def test_merge_domain(self, merged_domain_test_data, domain_mapping_test_data):
        expected_output = merged_domain_test_data

        input_data = expected_output.drop(columns="domain")

        actual_output = merge_domain(
            input_data,
            domain_mapping_test_data,
            "sic",
            "classification",
        )

        assert_frame_equal(actual_output, expected_output)
