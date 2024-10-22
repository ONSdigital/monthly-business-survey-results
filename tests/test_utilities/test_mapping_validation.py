import pandas as pd
import pytest

from mbs_results.utilities.mapping_validation import mapping_validation

scenario_path_prefix = "tests/data/"


def test_mapping_validation():
    df = pd.DataFrame({"name": ["a", "b", "c", "d"], "sic": [1, 2, 3, 4]})

    mapping_path = scenario_path_prefix + "utilities_data/mapping_missing.csv"
    with pytest.warns(UserWarning):
        mapping_validation(
            df=df,
            mapping_path=mapping_path,
            df_column_name="sic",
            mapping_file_column_name="sic",
        )
