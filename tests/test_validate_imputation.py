from pathlib import Path

import pandas as pd
import pytest
from helper_functions import does_not_raise

from mbs_results.validate_imputation import validate_imputation


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/validate_imputation")


@pytest.fixture(scope="class")
def missing_target_values_data(filepath):
    return pd.read_csv(filepath / "target_missing_values.csv", index_col=False)


class TestValidateImputation:
    @pytest.mark.parametrize(
        "target_column_name,expectation",
        [
            ("no_missing", does_not_raise()),
            ("one_missing", pytest.raises(ValueError)),
            ("all_missing", pytest.raises(ValueError)),
        ],
    )
    def test_target_missing_values_validation(
        self,
        missing_target_values_data,
        target_column_name,
        expectation,
    ):
        with expectation:
            validate_imputation(missing_target_values_data, target_column_name)
