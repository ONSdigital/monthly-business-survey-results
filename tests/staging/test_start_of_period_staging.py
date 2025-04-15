from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.data_cleaning import enforce_datatypes
from mbs_results.staging.stage_dataframe import start_of_period_staging


@pytest.fixture()
def filepath():
    return Path("tests/data/outputs/selective_editing")


@pytest.fixture()
def imputation_output(filepath):
    return pd.read_csv(
        filepath / "inputs/imputation_output_v0.1.1_test_snaphot.csv",
        index_col=False,
        dtype={"reference": int, "questioncode": int},
    )


@pytest.fixture()
def start_of_period_staging_output(filepath):
    return pd.read_csv(filepath / "start_of_period_staging_output.csv", index_col=False)


input_path = "tests/data/outputs/selective_editing/inputs/"

config = {
    "platform": "network",
    "bucket": "",
    "folder_path": input_path,
    "current_period": 202201,
    "revision_window": 1,
    "finalsel_keep_cols": [
        "formtype",
        "cell_no",
        "froempment",
        "frosic2007",
        "frotover",
        "reference",
    ],
    "idbr_to_spp": {
        "106": 12,
    },
    "form_id_spp": "form_type_spp",
    "form_id_idbr": "formtype",
    "reference": "reference",
    "period": "period",
    "auxiliary": "frotover",
    "auxiliary_converted": "converted_frotover",
    "question_no": "questioncode",
    "target": "adjustedresponse",
    "cell_number": "cell_no",
    "master_column_type_dict": {
        "reference": "int",
        "period": "date",
        "response": "float",
        "questioncode": "int",
        "adjustedresponse": "float",
        "frozensic": "str",
        "frozenemployees": "int",
        "frozenturnover": "float",
        "cellnumber": "int",
        "formtype": "str",
        "status": "str",
        "statusencoded": "int",
        "frosic2007": "str",
        "froempment": "int",
        "frotover": "float",
        "cell_no": "int",
    },
    "temporarily_remove_cols": [],
    "output_path": "",
    "sic": "frosic2007",
}


@pytest.fixture
def mock_to_csv():
    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        yield mock_to_csv


@pytest.fixture
def mock_read_and_combine_colon_sep_files():
    with patch(
        "mbs_results.staging.stage_dataframe.read_and_combine_colon_sep_files"
    ) as mock_read_and_combine_colon_sep_files:
        mock_read_and_combine_colon_sep_files.return_value = pd.DataFrame(
            {
                "reference": [
                    1,
                ],
                "formtype": [106],
                "cell_no": [999],
                "frosic2007": [999],
                "frotover": [999],
                "froempment": [999],
                "period": [202202],
            }
        )
        yield mock_read_and_combine_colon_sep_files


def test_start_of_period_staging(
    imputation_output,
    start_of_period_staging_output,
    mock_to_csv,
    mock_read_and_combine_colon_sep_files,
):
    expected_output = start_of_period_staging_output
    expected_output = enforce_datatypes(
        expected_output, keep_columns=expected_output.columns, **config
    )
    expected_output["period"] = (
        expected_output["period"].dt.strftime("%Y%m").astype(int)
    )

    config["selective_editing_period"] = (
        pd.to_datetime(config["current_period"], format="%Y%m")
        + pd.DateOffset(months=1)
    ).strftime("%Y%m")

    testing_input = enforce_datatypes(
        imputation_output, keep_columns=imputation_output.columns, **config
    )
    testing_input["period"] = testing_input["period"].dt.strftime("%Y%m").astype(int)
    actual_output = start_of_period_staging(testing_input, config)
    actual_output = actual_output[expected_output.columns]
    # Reorder columns to match expected output and selecting only the  needed
    # columns in expected output

    assert_frame_equal(
        actual_output, expected_output, check_like=True, check_dtype=False
    )
