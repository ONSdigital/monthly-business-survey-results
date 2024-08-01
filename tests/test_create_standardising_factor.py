from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.selective_editing import create_standardising_factor


@pytest.fixture(scope="class")
def filepath():
    return Path("tests")


@pytest.fixture(scope="class")
def create_standardising_factor_data(filepath):
    return pd.read_csv(
        filepath / "create_standardising_factor_data.csv", index_col=False
    )


class TestCreateStandardisingFactor:
    def test_create_standardising_factor(
        self,
        create_standardising_factor_data,
    ):
        expected_output = create_standardising_factor_data[
            create_standardising_factor_data["standardising_factor"].notna()
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

        input_data = create_standardising_factor_data.drop(
            columns="standardising_factor"
        )

        actual_output = create_standardising_factor(
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
