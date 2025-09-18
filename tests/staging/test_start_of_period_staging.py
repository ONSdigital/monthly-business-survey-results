from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.estimation.estimate import estimate
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import get_additional_outputs_df
from mbs_results.staging.data_cleaning import enforce_datatypes
from mbs_results.staging.stage_dataframe import start_of_period_staging


@pytest.fixture(scope="class")
def data_dir():
    return Path("tests/data/outputs/selective_editing")


@pytest.fixture(scope="class")
def imputation_output(data_dir):
    return pd.read_csv(
        data_dir / "inputs/imputation_output_v0.1.1_test_snapshot.csv",
        index_col=False,
        dtype={"reference": int, "questioncode": int},
    )


@pytest.fixture(scope="class")
def imputation_output_changing_formtypes(data_dir):
    return pd.read_csv(
        data_dir / "inputs/imputation_output_v0.1.1_test_snapshot_change_formtypes.csv",
        index_col=False,
        dtype={"reference": int, "questioncode": int},
    )


@pytest.fixture(scope="class")
def start_of_period_staging_output(data_dir):
    return pd.read_csv(data_dir / "start_of_period_staging_output.csv", index_col=False)


@pytest.fixture(scope="class")
def start_of_period_staging_output_changing_formtypes(data_dir):
    return pd.read_csv(
        data_dir / "start_of_period_staging_output_changing_formtypes.csv",
        index_col=False,
    )


input_path = "tests/data/outputs/selective_editing/inputs/"

config = {
    "platform": "network",
    "bucket": "",
    "idbr_folder_path": input_path,
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
        "201": 9,
        "202": 9,
        "203": 10,
        "204": 10,
        "205": 11,
        "216": 11,
        "106": 12,
        "111": 12,
        "117": 13,
        "167": 13,
        "123": 14,
        "173": 14,
        "817": 15,
        "867": 15,
        "823": 16,
        "873": 16,
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
    "population_prefix": "universe",
    "sample_prefix": "finalsel",
}


@pytest.fixture(scope="class")
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


@pytest.fixture(scope="class")
def mock_read_and_combine_colon_sep_files_changing_formtypes():
    with patch(
        "mbs_results.staging.stage_dataframe.read_and_combine_colon_sep_files"
    ) as mock_read_and_combine_colon_sep_files:
        mock_read_and_combine_colon_sep_files.return_value = pd.DataFrame(
            {
                "reference": [1, 2, 3],
                "formtype": [117, 817, 117],
                "cell_no": [999, 888, 777],
                "frosic2007": [999, 888, 777],
                "frotover": [999, 888, 777],
                "froempment": [999, 888, 777],
                "period": [202202, 202202, 202202],
            }
        )
        yield mock_read_and_combine_colon_sep_files


@patch("pandas.DataFrame.to_csv")  # mock pandas export csv function
class TestStartPeriodStaging:
    def test_start_of_period_staging(
        self,
        mock_to_csv,
        imputation_output,
        start_of_period_staging_output,
        mock_read_and_combine_colon_sep_files,
    ):
        config["current_period"] = 202201
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
        testing_input["period"] = (
            testing_input["period"].dt.strftime("%Y%m").astype(int)
        )
        actual_output = start_of_period_staging(testing_input, config)
        actual_output = actual_output[expected_output.columns]
        # Reorder columns to match expected output and selecting only the  needed
        # columns in expected output

        # mock_to_csv.assert_called_once_with(
        #     "export.csv", index=False, date_format="%Y%m"
        # )

        assert_frame_equal(
            actual_output, expected_output, check_like=True, check_dtype=False
        )

    def test_start_of_period_staging_changing_formtypes(
        self,
        mock_to_csv,
        imputation_output_changing_formtypes,
        start_of_period_staging_output_changing_formtypes,
        mock_read_and_combine_colon_sep_files_changing_formtypes,
    ):
        # Resetting current period, this is overwritten during start_of_period_staging
        config["current_period"] = 202201
        expected_output = start_of_period_staging_output_changing_formtypes.copy()
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
            imputation_output_changing_formtypes,
            keep_columns=imputation_output_changing_formtypes.columns,
            **config,
        )
        testing_input["period"] = (
            testing_input["period"].dt.strftime("%Y%m").astype(int)
        )
        actual_output = start_of_period_staging(testing_input, config)
        actual_output = actual_output[expected_output.columns]
        # Reorder columns to match expected output and selecting only the  needed
        # columns in expected output

        actual_output = actual_output.sort_values(
            ["reference", "period", "questioncode"], ignore_index=True
        )

        expected_output = expected_output.sort_values(
            ["reference", "period", "questioncode"], ignore_index=True
        )

        assert_frame_equal(
            actual_output, expected_output, check_like=True, check_dtype=False
        )

    @pytest.mark.skip(reason="Test not implemented yet")
    def test_start_of_period_staging_changing_formtypes_integreation_test(
        self,
        mock_to_csv,
        imputation_output_changing_formtypes,
        start_of_period_staging_output_changing_formtypes,
        mock_read_and_combine_colon_sep_files_changing_formtypes,
    ):
        config["current_period"] = 202201
        expected_output = start_of_period_staging_output_changing_formtypes.copy()
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
            imputation_output_changing_formtypes,
            keep_columns=imputation_output_changing_formtypes.columns,
            **config,
        )
        testing_input["period"] = (
            testing_input["period"].dt.strftime("%Y%m").astype(int)
        )
        staged_output = start_of_period_staging(testing_input, config)

        staged_output.rename(
            columns={"imputed_and_derived_flag": "imputation_flags_adjustedresponse"},
            inplace=True,
        )

        estimation_output = estimate(staged_output, config)

        outlier_output = detect_outlier(estimation_output, config)

        se_outputs_df = get_additional_outputs_df(estimation_output, outlier_output)

        se_outputs_df.to_csv(
            config["output_path"]
            + f"se_outputs_full_df_{config['period_selected']}_test_result.csv",
            index=False,
        )


# Known issues:
# Zero on q40 as this has been derived from q46 and q47
# If we fix this in start of period staging, what will happen when constrains are
# applied again in outliering?
