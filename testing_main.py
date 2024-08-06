from importlib import metadata
import pandas as pd
import numpy as np
import json

from testing_helpers import get_pre_impute_data, proccess_for_pre_impute, check_na_duplicates,map_form_type

# pip install git+https://github.com/ONSdigital/monthly-business-survey-results.git

from mbs_results.data_cleaning import create_imputation_class
from mbs_results.ratio_of_means import ratio_of_means
from mbs_results.constrains import constrain
from mbs_results.apply_estimation import apply_estimation

from mbs_results.flag_for_winsorisation import winsorisation_flag #1
from mbs_results.calculate_predicted_unit_value import calculate_predicted_unit_value #2
from mbs_results.calculate_ratio_estimation import calculate_ratio_estimation #3
from mbs_results.calculate_winsorised_weight import calculate_winsorised_weight #4


def wrap_winsorised(df,l_values_path):
    """Temporary wrap"""

    l_values = pd.read_csv(l_values_path)
    
    df = df.drop_duplicates(subset=['period','reference','question_no'],keep = "last")

    # Imputed values are in a seperate column 
    df["adjusted_value"] = df[["adjusted_value", "imputed_value"]].agg(
          sum, axis=1
      )
    df1 = winsorisation_flag(df,"design_weight","calibration_factor")
    
    df2 = calculate_predicted_unit_value(df1,"frotover_y","sampled","design_weight","adjusted_value",'nw_ag_flag')
    
    df2 = pd.merge(df2,l_values,how="left",left_on=["period","question_no","frosic2007"],right_on=["period","question_no","classification"])

    df2['frosic2007_3d'] = (pd.Series(np.floor(df2['frosic2007']/100 )*100)).astype(int)
 
    
    df3 = calculate_ratio_estimation(df2,"frotover_y","sampled","design_weight","calibration_factor","adjusted_value","predicted_unit_value",
                                     "l_value",'nw_ag_flag')
    
    df4 = calculate_winsorised_weight(df3,"frosic2007_3d","period","frotover_y","sampled","design_weight","calibration_factor","adjusted_value","predicted_unit_value",
                                      "l_value","ratio_estimation_treshold","nw_ag_flag")


    return df4

def load_config():
        with open('test_outputs_config.json') as f:
            return json.load(f)
    
if __name__ == "__main__":
    
    FILE_VERSION = metadata.metadata("monthly-business-survey-results")["version"]
    
    config = load_config()
    config['calibration_group_map'] = pd.read_csv(config['calibration_group_map'])
    
    pre_impute_df = get_pre_impute_data(
        config['df_path'],config['df_path'],config['sample_path'],config['sample_column_names'])
    
    pre_impute_df = proccess_for_pre_impute(pre_impute_df)
    
    check_na_duplicates(pre_impute_df) #just basic check
    
    pre_impute_df = create_imputation_class(pre_impute_df,"cell_no","class")

    post_impute = pre_impute_df.groupby("question_no").apply(
        lambda df:ratio_of_means(
            df = df,
            reference="reference",
            target="adjusted_value",
            period="period",
            strata = "class",
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

    # cell_no frotover exist in both dfs we load twice finalsel
    # cell_no_x is original no changes, cell_no_y from estimation (some changes applied for NI and UK)
    # frotover_x has na for derived values frotover_y has original values
    
    post_estimate = pd.merge(post_constrain,estimate_df,how = "left",on = ["period","reference"])
            
    estimate_out = post_estimate[["period","cell_no_y","calibration_group","design_weight","calibration_factor"]]
    
    estimate_out.to_csv(config['out_path']+f"estimation_output_{FILE_VERSION}.csv",index=False)
        
    post_win = wrap_winsorised(post_estimate,config['l_values_path'])
    
    post_win.to_csv(config['out_path']+f"winsorisation_output_{FILE_VERSION}.csv",index=False)

    
