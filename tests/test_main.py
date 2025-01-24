import os

from helper_functions import create_testing_config

from mbs_results.main import run_mbs_main

input_path = "data/test_main/input/"

test_config = {
    "calibration_group_map_path": input_path
    + "test_cell_no_calibration_group_mapping.csv",
    "classification_values_path": input_path + "test_classification_sic_mapping.csv",
    "folder_path": input_path,
    "l_values_path": input_path
    + "test_classification_question_number_l_value_mapping.csv",
    "manual_constructions_path": input_path + "test_manual_constructions.csv",
    "mbs_file_name": "test_snaphot.json",
    "output_path": "data/test_main/output/",
    "population_path": input_path + "test_universe009_*",
    "sample_path": input_path + "test_finalsel009_*",
    "back_data_qv_path": input_path + "test_qv_009_202112.csv",
    "back_data_cp_path": input_path + "input/test_cp_009_202112.csv",
    "back_data_finalsel_path": input_path + "/test_finalsel009_202112",
    "period_selected": 202206,
    "current_period": 202206,
    "previous_period": 202205,
    "revision_period": 6,
}


def test_main():
    """Testing if main works, this test aims to check if all methods are
    integrated together. Updating config to match testing data, also saving
    config in tests directory.
    """
    create_testing_config(test_config)

    original_dir = os.getcwd()

    # only testing main needs this as working dir
    # other tests expect as working dir the parent dir
    os.chdir("tests/")

    run_mbs_main()

    os.chdir(original_dir)
