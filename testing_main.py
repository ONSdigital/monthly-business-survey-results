from importlib import metadata
import pandas as pd

from testing_helpers import (
    get_pre_impute_data, proccess_for_pre_impute,
    check_na_duplicates,map_form_type,get_qa_output_482,
    load_config,join_l_values,extract_mannual_constructed
    )

# pip install git+https://github.com/ONSdigital/monthly-business-survey-results.git@0.0.2

from mbs_results.data_cleaning import create_imputation_class
from mbs_results.ratio_of_means import ratio_of_means
from mbs_results.constrains import constrain
from mbs_results.apply_estimation import apply_estimation
from mbs_results.winsorisation import winsorise

    
if __name__ == "__main__":
        
    FILE_VERSION = metadata.metadata("monthly-business-survey-results")["version"]
    
    config = load_config()
    config['calibration_group_map'] = pd.read_csv(config['calibration_group_map'])
    
    pre_impute_df = get_pre_impute_data(
        config['df_path'],config['df_path'],config['sample_path'],config['sample_column_names'])
    
    man_df = extract_mannual_constructed(pre_impute_df)

    pre_impute_df = proccess_for_pre_impute(pre_impute_df)
    
    check_na_duplicates(pre_impute_df) #just basic check
    
    pre_impute_df = create_imputation_class(pre_impute_df,"cell_no","imputation_class")
    
    post_impute = pre_impute_df.groupby("question_no").apply(
        lambda df:ratio_of_means(
            df = df,
            manual_constructions = man_df,
            reference="reference",
            target="adjusted_value",
            period="period",
            strata = "imputation_class",
            auxiliary="frotover"))
    
    post_impute['period']  =  post_impute['period'].dt.strftime('%Y%m').astype("int")

    post_impute = post_impute.reset_index(drop=True) #remove groupby leftovers
    
    check_na_duplicates(post_impute) #just basic check

    post_impute = map_form_type(post_impute)
    
    post_constrain = constrain(post_impute,"period","reference","adjusted_value","imputed_value","question_no","form_type_spp")

    check_na_duplicates(post_constrain) #just basic check

    # Imputation test data here
    post_constrain.to_csv(config['out_path']+f"imputation_output_{FILE_VERSION}.csv",index=False)
    
    estimate_df = apply_estimation(**config)

    estimate_df = estimate_df.drop(columns = ["cell_no","frotover"])

    post_estimate = pd.merge(post_constrain,estimate_df,how = "left",on = ["period","reference"])
            
    estimate_out = post_estimate[["period","cell_no","calibration_group","design_weight","calibration_factor"]]
        
    estimate_out.to_csv(config['out_path']+f"estimation_output_{FILE_VERSION}.csv",index=False)
    
    post_win = join_l_values(post_estimate,config['l_values_path'],config["classification_values_path"])
                
    post_win = winsorise(
            post_win,
            "calibration_group",
            "period",
            "frotover",
            "sampled",
            "design_weight",
            "calibration_factor",
            "adjusted_value",
            "l_value",
        )

    post_win.to_csv(config['out_path']+f"winsorisation_output_{FILE_VERSION}.csv",index=False)

    asap_482_df = get_qa_output_482(post_win)
    
    asap_482_df.to_csv(config['out_path']+f"asap_482_df_{FILE_VERSION}.csv",index=False)
