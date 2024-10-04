import pandas as pd 
from mbs_results.unsorted.selective_editing import calculate_predicted_value, create_standardising_factor
from mbs_results.merge_domain import merge_domain



if __name__ == "__main__":
    sharepoint_path = ""
    sic_domain_mapping = pd.read_csv(sharepoint_path + "mapping_files/sic_domain_mapping.csv").astype(int)
    wins_output = pd.read_csv(sharepoint_path + "winsorisation/winsorisation_output_0.0.2.csv")
    # print(sic_domain_mapping,wins_output.head(10))
    from mbs_results.merge_domain import merge_domain
    test_merge = merge_domain(
        wins_output,
        sic_domain_mapping,
        "sic_5_digit",
        "sic_5_digit"
    )

    # print(test_merge.columns)
    test_merge = calculate_predicted_value(test_merge,"imputed_value","adjusted_value")
    standardising_factor = create_standardising_factor(dataframe=test_merge,
                                reference="reference",
                                period="period",
                                domain="domain",
                                question_no="question_no",
                                predicted_value="predicted_value",
                                imputation_marker="imputation_flags_adjusted_value",
                                a_weight="design_weight", # a_weight?
                                o_weight="outlier_weight",
                                g_weight="calibration_factor", #g_weight?
                                auxiliary_value="frotover",
                                previous_period=202201
                                )
    print(standardising_factor.head())