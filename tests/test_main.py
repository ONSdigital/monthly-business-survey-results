import os.path
from glob import glob

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.main import produce_additional_outputs_wrapper, run_mbs_main

input_path = "tests/data/test_main/input/"

test_config = {
    "platform": "network",
    "bucket": "",
    "back_data_format": "csv",
    "calibration_group_map_path": input_path
    + "test_cell_no_calibration_group_mapping.csv",
    "classification_values_path": input_path + "test_classification_sic_mapping.csv",
    "snapshot_file_path": input_path + "test_snaphot.json",
    "idbr_folder_path": input_path,
    "l_values_path": input_path
    + "test_classification_question_number_l_value_mapping.csv",
    "manual_constructions_path": input_path + "test_manual_constructions.csv",
    "filter": input_path + "filters.csv",
    "manual_outlier_path": None,
    "output_path": "tests/data/test_main/output/",
    "population_prefix": "test_universe009",
    "sample_prefix": "test_finalsel009",
    "population_counts_prefix": "population_counts",
    "back_data_qv_path": input_path + "test_qv_009_202112.csv",
    "back_data_cp_path": input_path + "test_cp_009_202112.csv",
    "back_data_qv_cp_json_path": input_path + "test_json_backdata.json",
    "back_data_finalsel_path": input_path + "test_finalsel009_202112",
    "lu_path": input_path + "ludets009_202303",
    "sic": "frosic2007",
    "current_period": 202206,
    "revision_window": 6,
    "state": "frozen",
    "devolved_nations": ["Scotland", "Wales"],
    "optional_outputs": ["all"],
    "cdid_data_path": "tests/data/outputs/csdb_output/cdid_mapping.csv",
    "generate_schemas": True,
    "schema_path": "tests/data/test_main/schemas/",
    "debug_mode": True,
    "run_id": "1",
}


def test_main():
    """
    Test the main function to ensure all methods are integrated correctly.
    Update the configuration to match the testing data and pass on the
    config_user_test as a parameter to run_mbs_main. The config
    dictionary will be updated in the mbs_results/utilites/inputs.py.
    """

    run_mbs_main(config_user_dict=test_config)

    out_path = "tests/data/test_main/output/"

    # check pattern due different version in file name
    patern = glob(out_path + "mbs_results_*.csv")

    actual = pd.read_csv(patern[0])
    expected = pd.read_csv(out_path + "expected_from_mbs_main.csv")
    for col in actual.select_dtypes(include=["int", "float"]).columns:
        actual[col] = actual[col].astype(float)
    for col in expected.select_dtypes(include=["int", "float"]).columns:
        expected[col] = expected[col].astype(float)

    assert_frame_equal(actual, expected)


# We want to avoid running it in the workflows
@pytest.mark.skipif(
    not (os.path.isfile("config_user.json")),
    reason="Aplicable only in CDP and data stored in S3",
)
def test_main_with_s3():
    """
    Triggers a main run when config_user.json exists in the project
    parent directory (some_path_to/monthly-business-survey-results/).
    By default the pipeline runs on `s3` platform, this pytest expects
    `config_user.json` to be filled properly and point to the testing data in
    s3.

    The testing is mainly useful for developers and reviewers to ensure pipeline
    integration with s3
    """
    run_mbs_main()


def test_main_json():
    config_user_test = test_config
    config_user_test["back_data_format"] = "json"

    run_mbs_main(config_user_dict=config_user_test)


def test_produce_additional_outputs_wrapper():
    """Triggers a produce_additional_outputs_wrapper run, the output
    of mbs main is the input of this"""

    test_outputs_config = {
        "platform": "network",
        "bucket": "",
        "idbr_folder_path": input_path,
        "snapshot_file_path": input_path + "test_snaphot.json",
        "main_mbs_output_folder_path": "tests/data/test_main/output/",
        "mbs_output_prefix": "mbs_results",
        "population_counts_prefix": "population_counts",
        "cdid_data_path": "tests/data/outputs/csdb_output/cdid_mapping.csv",
        "output_path": "tests/data/test_main/output/",
        "ludets_prefix": "ludets009_",
        "current_period": 202206,
        "revision_window": 6,
        "devolved_nations": ["Scotland", "Wales"],
        "run_id": "1",
    }
    produce_additional_outputs_wrapper(config_user_dict=test_outputs_config)
