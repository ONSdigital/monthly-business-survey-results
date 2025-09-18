import pandas as pd
import pytest

from mbs_results.imputation.validate_imputation import validate_imputation
from tests.helper_functions import does_not_raise


@pytest.fixture(scope="class")
def data_dir(imputation_data_dir):
    return imputation_data_dir / "validate_imputation"


@pytest.fixture(scope="class")
def missing_target_values_data(data_dir):
    return pd.read_csv(data_dir / "target_missing_values.csv", index_col=False)


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
