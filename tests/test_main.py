import os.path

import pytest

from mbs_results.main import run_mbs_main

input_path = "tests/data/test_main/input/"

test_config = {
    "platform": "network",
    "bucket": "",
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
    "back_data_qv_path": input_path + "test_qv_009_202112.csv",
    "back_data_cp_path": input_path + "test_cp_009_202112.csv",
    "back_data_finalsel_path": input_path + "test_finalsel009_202112",
    "lu_path": input_path + "ludets009_202303",
    "sic": "frosic2007",
    "current_period": 202206,
    "revision_window": 6,
    "snapshot_alternate_path_OPTIONAL": None,
    "state": "frozen",
    "devolved_nations": ["Scotland", "Wales"],
    "optional_outputs": ["all"],
    "cdid_data_path": "tests/data/outputs/csdb_output/cdid_mapping.csv",
}


def test_main():
    """
    Test the main function to ensure all methods are integrated correctly.
    Update the configuration to match the testing data and pass on the
    config_user_test as a parameter to run_mbs_main. The config
    dictionary will be updated in the mbs_results/utilites/inputs.py.
    """
    # Create config dictionary
    config_user_test = test_config

    run_mbs_main(config_user_dict=config_user_test)


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
