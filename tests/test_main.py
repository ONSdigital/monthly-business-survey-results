from mbs_results.main import run_mbs_main

input_path = "tests/data/test_main/input/"

test_config = {
    "platform": "network",
    "bucket": "",
    "calibration_group_map_path": input_path
    + "test_cell_no_calibration_group_mapping.csv",
    "classification_values_path": input_path + "test_classification_sic_mapping.csv",
    "folder_path": input_path,
    "l_values_path": input_path
    + "test_classification_question_number_l_value_mapping.csv",
    "manual_constructions_path": input_path + "test_manual_constructions.csv",
    "mbs_file_name": "test_snaphot.json",
    "output_path": "tests/data/test_main/output/",
    "population_path": input_path + "test_universe009_*",
    "sample_path": input_path + "test_finalsel009_*",
    "back_data_qv_path": input_path + "test_qv_009_202112.csv",
    "back_data_cp_path": input_path + "test_cp_009_202112.csv",
    "back_data_finalsel_path": input_path + "test_finalsel009_202112",
    "period_selected": 202206,
    "current_period": 202206,
    "previous_period": 202205,
    "revision_period": 6,
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
