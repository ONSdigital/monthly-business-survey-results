from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.pivot_imputation_value import pivot_imputation_value


@pytest.fixture(scope="class")
def filepath():
    return Path("tests")


@pytest.fixture(scope="class")
def pivot_imputation_value_input(filepath):
    return pd.read_csv(
        filepath / "data"/"pivot_imputation_value_input.csv", index_col=False
    )
  
@pytest.fixture(scope="class")
def pivot_imputation_value_output(filepath):
    return pd.read_csv(
        filepath / "data"/"pivot_imputation_value_output.csv", index_col=False
    )


class TestPivotImputationValue:
    def test_pivot_imputation_value(
        self,
        pivot_imputation_value_input,
        pivot_imputation_value_output
    ):
        expected_output = pivot_imputation_value_output[
           [
                "date",
                "sic",
                "cell",
                "question",
                "link_type",
                "imputation_link",
            ]
        ].reset_index(drop=True)
        
        expected_output["link_type"] = pd.Categorical(expected_output["link_type"], categories=["F", "B", "C"], ordered=True)

        input_data = pivot_imputation_value_input.drop(
            columns=["identifier"]
        )

        actual_output = pivot_imputation_value(
            input_data,
            "identifier",
            "date",
            "sic",
            "cell",
            "forward",
            "backward",
            "construction",
            "question",
        )

        assert_frame_equal(actual_output, expected_output)
