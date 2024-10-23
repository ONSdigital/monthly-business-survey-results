# `tests` folder overview

tests follow mbs_results file structure 

data follow tests structure, the lowest level folder is the testing function with test cases within


Tests tree:
```
+---data
|   +---archive
|   |       apply_imputation_link.csv
|   |       BIR.csv
|   |       colon_sep_202401
|   |       C_FIC.csv
|   |       FIR.csv
|   |
|   +---estimation
|   |   +---calculate_estimation_weights
|   |   |       calibration_factor_combined.csv
|   |   |       calibration_factor_separate.csv
|   |   |       design_weights.csv
|   |   |
|   |   +---pre_processing_estimation
|   |   |       derive_estimation_variables.csv
|   |   |
|   |   \---validate_estimation
|   |           validate_estimation.csv
|   |
|   +---imputation
|   |   +---apply_imputation_link
|   |   |       FIR_BIR_C_FIC.csv
|   |   |       MC_FIMC.csv
|   |   |
|   |   +---calculate_imputation_link
|   |   |       construction.csv
|   |   |       forward_backward.csv
|   |   |
|   |   +---construction_matches
|   |   |       construction_matches.csv
|   |   |
|   |   +---cumulative_imputation_links
|   |   |       cumulative_links.csv
|   |   |
|   |   +---flag_and_count_matched_pairs
|   |   |       count_matches_expected_output.csv
|   |   |       count_matches_input.csv
|   |   |       flag_pairs_2_groups_expected_output.csv
|   |   |       flag_pairs_expected_output.csv
|   |   |       flag_pairs_missing_rows_expected_output.csv
|   |   |
|   |   +---imputation_flags
|   |   |       imputation_flag_data.csv
|   |   |       imputation_flag_data_manual_construction.csv
|   |   |
|   |   +---link_filter
|   |   |       test_flag_data.csv
|   |   |       test_flag_filters.csv
|   |   |
|   |   +---predictive_variable
|   |   |       predictive_variable.csv
|   |   |
|   |   +---ratio_of_means
|   |   |   |   01_C_input.csv
|   |   |   |   01_C_output.csv
|   |   |   |   02_C_FI_input.csv
|   |   |   |   02_C_FI_output.csv
|   |   |   |   03_R_R_FI_input.csv
|   |   |   |   03_R_R_FI_output.csv
|   |   |   |   04_R_R_FI_FI_input.csv
|   |   |   |   04_R_R_FI_FI_output.csv
|   |   |   |   05_R_R_FI_FI_FI_year_span_input.csv
|   |   |   |   05_R_R_FI_FI_FI_year_span_output.csv
|   |   |   |   06_BI_BI_R_input.csv
|   |   |   |   06_BI_BI_R_output.csv
|   |   |   |   07_BI_BI_R_FI_FI_R_FI_input.csv
|   |   |   |   07_BI_BI_R_FI_FI_R_FI_output.csv
|   |   |   |   08_R_R_R_input.csv
|   |   |   |   08_R_R_R_output.csv
|   |   |   |   09_R_NS_C_input.csv
|   |   |   |   09_R_NS_C_output.csv
|   |   |   |   10_C_FI_NS_R_input.csv
|   |   |   |   10_C_FI_NS_R_output.csv
|   |   |   |   11_R_R_FI-BI_R_R_input.csv
|   |   |   |   11_R_R_FI-BI_R_R_output.csv
|   |   |   |   12_C_FI_FI_FI_FI_input.csv
|   |   |   |   12_C_FI_FI_FI_FI_output.csv
|   |   |   |   13_R_FI_FI_NS_BI_BI_R_input.csv
|   |   |   |   13_R_FI_FI_NS_BI_BI_R_output.csv
|   |   |   |   14_C_FI_FI_NS_BI_BI_R_input.csv
|   |   |   |   14_C_FI_FI_NS_BI_BI_R_output.csv
|   |   |   |   15_BI_BI_R_NS_R_FI_FI_input.csv
|   |   |   |   15_BI_BI_R_NS_R_FI_FI_output.csv
|   |   |   |   16_BI_BI_R_NS_C_FI_FI_input.csv
|   |   |   |   16_BI_BI_R_NS_C_FI_FI_output.csv
|   |   |   |   17_NS_R_FI_NS_input.csv
|   |   |   |   17_NS_R_FI_NS_output.csv
|   |   |   |   18_NS_BI_R_NS_input.csv
|   |   |   |   18_NS_BI_R_NS_output.csv
|   |   |   |   19_link_columns_input.csv
|   |   |   |   19_link_columns_output.csv
|   |   |   |   20_mixed_data_input.csv
|   |   |   |   20_mixed_data_output.csv
|   |   |   |   21_class_change_R_C_FI_input.csv
|   |   |   |   21_class_change_R_C_FI_output.csv
|   |   |   |   22_class_change_C_BI_R_input.csv
|   |   |   |   22_class_change_C_BI_R_output.csv
|   |   |   |   23_class_change_C_C_FI_input.csv
|   |   |   |   23_class_change_C_C_FI_output.csv
|   |   |   |   24_class_change_R_BI_R_input.csv
|   |   |   |   24_class_change_R_BI_R_output.csv
|   |   |   |   25_class_change_C_FI_FI_input.csv
|   |   |   |   25_class_change_C_FI_FI_output.csv
|   |   |   |   26_C_FI_FI_NS_BI_BI_R_filtered_input.csv
|   |   |   |   26_C_FI_FI_NS_BI_BI_R_filtered_output.csv
|   |   |   |   27_BI_BI_R_NS_R_FI_FI_filtered_input.csv
|   |   |   |   27_BI_BI_R_NS_R_FI_FI_filtered_output.csv
|   |   |   |   28_link_columns_filtered_input.csv
|   |   |   |   28_link_columns_filtered_output.csv
|   |   |   |   29_mixed_data_filtered_input.csv
|   |   |   |   29_mixed_data_filtered_output.csv
|   |   |   |   30_class_change_C_C_FI_filtered_input.csv
|   |   |   |   30_class_change_C_C_FI_filtered_output.csv
|   |   |   |   31_no_response_input.csv
|   |   |   |   31_no_response_output.csv
|   |   |   |   32_divide_by_zero_input.csv
|   |   |   |   32_divide_by_zero_output.csv
|   |   |   |   33_multi_variable_C_BI_R_input.csv
|   |   |   |   33_multi_variable_C_BI_R_output.csv
|   |   |   |   34_multi_variable_C_BI_R_filtered_input.csv
|   |   |   |   34_multi_variable_C_BI_R_filtered_output.csv
|   |   |   |   35_BI_BI_R_FI_FI_R_FI_alternating_filtered_input.csv
|   |   |   |   35_BI_BI_R_FI_FI_R_FI_alternating_filtered_output.csv
|   |   |   |   rom_test_data_case_mc_10_input.csv
|   |   |   |   rom_test_data_case_mc_10_output.csv
|   |   |   |   rom_test_data_case_mc_1_input.csv
|   |   |   |   rom_test_data_case_mc_1_output.csv
|   |   |   |   rom_test_data_case_mc_2_input.csv
|   |   |   |   rom_test_data_case_mc_2_output.csv
|   |   |   |   rom_test_data_case_mc_3_input.csv
|   |   |   |   rom_test_data_case_mc_3_output.csv
|   |   |   |   rom_test_data_case_mc_4_input.csv
|   |   |   |   rom_test_data_case_mc_4_output.csv
|   |   |   |   rom_test_data_case_mc_5_input.csv
|   |   |   |   rom_test_data_case_mc_5_output.csv
|   |   |   |   rom_test_data_case_mc_6_input.csv
|   |   |   |   rom_test_data_case_mc_6_output.csv
|   |   |   |   rom_test_data_case_mc_7_input.csv
|   |   |   |   rom_test_data_case_mc_7_output.csv
|   |   |   |   rom_test_data_case_mc_8_input.csv
|   |   |   |   rom_test_data_case_mc_8_output.csv
|   |   |   |   rom_test_data_case_mc_9_input.csv
|   |   |   |   rom_test_data_case_mc_9_output.csv
|   |   |   |
|   |   |   \---ratio_of_means_filters
|   |   |           26_C_FI_FI_NS_BI_BI_R_filtered.csv
|   |   |           27_BI_BI_R_NS_R_FI_FI_filtered.csv
|   |   |           28_link_columns_filtered.csv
|   |   |           29_mixed_data_filtered.csv
|   |   |           30_class_change_C_C_FI_filtered.csv
|   |   |           34_multi_variable_C_BI_R_filtered.csv
|   |   |           35_BI_BI_R_FI_FI_R_FI_alternating_filtered.csv
|   |   |
|   |   \---validate_imputation
|   |           target_missing_values.csv
|   |
|   +---outlier_detection
|   |   +---calculate_predicted_unit_value
|   |   |       predicted_unit_value_data.csv
|   |   |       predicted_unit_value_output.csv
|   |   |
|   |   +---calculate_ratio_estimation
|   |   |       ratio_estimation_data.csv
|   |   |       ratio_estimation_data_output.csv
|   |   |
|   |   +---calculate_winsorised_weight
|   |   |       winsorised_weight_data.csv
|   |   |       winsorised_weight_data_output.csv
|   |   |
|   |   +---flag_for_winsorisation
|   |   |       flag_data.csv
|   |   |
|   |   +---replace_l_values
|   |   |       replace_l_values.csv
|   |   |       replace_l_values_input.csv
|   |   |
|   |   \---test_winsorisation
|   |           winsorised_weight_data_output.csv
|   |
|   +---outputs
|   |   +---pivot_imputation_value
|   |   |       count_data_input.csv
|   |   |       merge_counts_output.csv
|   |   |       pivot_imputation_value_output.csv
|   |   |
|   |   +---selective_editing
|   |   |       calculate_predicted_value_data.csv
|   |   |       create_standardising_factor_data.csv
|   |   |
|   |   \---turnover_analysis
|   |           cp_input.csv
|   |           create_standardising_factor_data.csv
|   |           finalsel_input.csv
|   |           qv_input.csv
|   |           turnover_analysis_output.csv
|   |           winsorisation_input.csv
|   |
|   +---staging
|   |   +---create_missing_questions
|   |   |       create_missing_questions_contributors.csv
|   |   |       create_missing_questions_output.csv
|   |   |       create_missing_questions_responses.csv
|   |   |
|   |   +---data_cleaning
|   |   |       imputation_flag_data.csv
|   |   |       test_correct_values.csv
|   |   |       test_create_imputation_class.csv
|   |   |       test_run_live_or_frozen.csv
|   |   |
|   |   \---merge_domain
|   |           domain_mapping.csv
|   |           merge_domain.csv
|   |
|   \---utilities
|       +---constrains
|       |       derived-questions-winsor-missing.csv
|       |       derived-questions-winsor.csv
|       |       test_replace_values_index_based.csv
|       |       test_sum_sub_df.csv
|       |
|       +---mapping_validation
|       |       mapping_missing.csv
|       |
|       \---read_colon_separated_file
|               colon_sep_202401
|
+---estimation
|       test_calculate_estimation_weights.py
|       test_pre_processing_estimation.py
|       test_validate_estimation.py
|
+---imputation
|       test_apply_imputation_link.py
|       test_calculate_imputation_link.py
|       test_construction_matches.py
|       test_cumulative_imputation_links.py
|       test_flag_and_count_matched_pairs.py
|       test_imputation_flags.py
|       test_link_filter.py
|       test_predictive_variable.py
|       test_ratio_of_means.py
|       test_validate_imputation.py
|
+---outlier_detection
|       test_calculate_predicted_unit_value.py
|       test_calculate_ratio_estimation.py
|       test_calculate_winsorised_weight.py
|       test_flag_for_winsorisation.py
|       test_replace_l_values.py
|       test_winsorisation.py
|
+---outputs
|       test_get_additional_outputs.py
|       test_pivot_imputation_value.py
|       test_selective_editing.py
|       test_turnover_analysis.py
|
+---staging
|       test_create_missing_questions.py
|       test_data_cleaning.py
|       test_merge_domain.py
|
\---utilities
        test_constrains.py
        test_mapping_validation.py
        test_utils.py
        test_validation_checks.py
```