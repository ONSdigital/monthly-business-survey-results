import pandas as pd
import pytest

from mbs_results.utilities.mapping_validation import mapping_validation


def test_mapping_validation(utilities_data_dir):
    df = pd.DataFrame({"name": ["a", "b", "c", "d"], "sic": [1, 2, 3, 4]})

    mapping_path = str(utilities_data_dir / "mapping_validation/mapping_missing.csv")
    with pytest.warns(UserWarning):
        mapping_validation(
            df=df,
            mapping_path=mapping_path,
            df_column_name="sic",
            mapping_file_column_name="sic",
        )
