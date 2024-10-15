import fnmatch
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
import glob

import json

	
# pip install git+https://github.com/ONSdigital/monthly-business-survey-results.git
from mbs_results.utilities.utils import read_colon_separated_file, convert_column_to_datetime


def get_patern_df(filepath, pattern):
    """Loads as pd dataframe all csv files with pattern"""
    
    filenames = [
        filename for filename in listdir(filepath) if isfile(join(filepath, filename))
    ]
    filenames = fnmatch.filter(filenames, pattern)
    df_list = [pd.read_csv(filepath + filename) for filename in filenames]
    df = pd.concat(df_list, ignore_index=True)

    return df



def get_pre_impute_data(cp_path,qv_path,finalsel_path,finalsel_columns):
    """Reads qv and left joins cp and finalsel data to qv"""
    
    qv_df = get_patern_df(qv_path,"qv*.csv")
    cp_df = get_patern_df(cp_path,"cp*.csv")

    df_finalsel = pd.concat(
        [read_colon_separated_file(f,finalsel_columns) for f in  glob.glob(finalsel_path)]
        ,ignore_index=True) 
    
    qv_and_cp = pd.merge(qv_df,cp_df,how = "left",on = ["period","reference"])
    pre_proccessed_df = pd.merge(qv_and_cp,df_finalsel,how = "left",on = ["period","reference"])
    
    return pre_proccessed_df


def proccess_for_pre_impute(df):
    """For ASAP-492"""
    
    df['period'] = convert_column_to_datetime(df['period'] )

    questions = [40, 42, 43, 46, 47, 49, 90, 110]
        
    df = df.loc[df['question_no'].isin(questions)]
    
    # has 60 columns, selecting only the ones we need for tidyness
    df = df[[
        'period', 'reference', 'question_no', 'adjusted_value',"cell_no",
        "frotover","form_type","response_type","type","error_mkr","froempment"]]

    # Updating response_type to 1 for 15399057545 , this is a known error check ASAP-492 for details
    df.loc[df['reference']==15399057545,"response_type"] = 1
    
    imputed_values_rules = (
        (df["type"].isin([3, 4, 5, 6])) & 
        (df["period"] != df["period"].min())
    )

    df.loc[imputed_values_rules, "adjusted_value"] = np.nan
    
    df["form_type"] = df["form_type"].str.strip() #remove whitespace

    remove_derived_rules = (
          ((df["form_type"].isin(["T117G","T167G","T123G","T173G"])) & (df["question_no"]==40))    
         | ((df["form_type"].isin(["T817G", "T867G"])) & (df["question_no"]==46))
         | ((df["form_type"].isin(["T823G","T873G" ])) & (df["question_no"]==42))

        )
    
    df.drop(df[remove_derived_rules].index, inplace=True)
    
    df.reset_index(drop=True,inplace=True)

    #Some checks to check if rules were applied
    nan_and_zero_check = df.loc[df["adjusted_value"].isna() | (df["adjusted_value"] == 0)]

    print("Verification after applying NaN rules on 'adjusted_value':")
    print("1. 'adjusted_value' set to NaN for type 3, 4, 5, 6, excluding first period:")
    print("   - All NaN entries by 'response_type' and 'type':")
    print(nan_and_zero_check.groupby(["response_type", "type"])["adjusted_value"].unique())
    
    return df
    
def check_na_duplicates(df):
    """Does basic checks"""
    
    print("nas: \n",df.isna().sum()) #count na
    mask = df.groupby(['period','reference','question_no'])['reference'].transform('size')>1
    print("Sum of duplicates: ",len(df[mask])) # check for duplicates
    
    
def map_form_type(df):
    
    form_type_map = {
    "T117G":13,
    "T167G":13,
    "T123G":14,
    "T173G":14,
    "T817G":15,
    "T867G":15,
    "T823G":16,
    "T873G":16
    }

 
    df["form_type"] = df["form_type"].str.strip()

    df = df.assign(form_type_spp= df["form_type"].map(form_type_map))
    
    return df

def get_qa_output_482(post_win_df):
    """For ASAP-482"""
    
    requested_columns = [
        'reference',
        'period',
        'frosic2007',
        'classification',
        'cell_no',
        'frotover',
        'froempment', 
        'form_type', 
        'response_type', 
        'question_no',
        'adjusted_value',
        'error_mkr', 
        'design_weight',
        'calibration_factor', 
        'outlier_weight',
        'total weight (A*G*O)',
        'weighted adjusted value', 
        'imputation_flags_adjusted_value',
        'imputation_class',
        'f_link_adjusted_value', 
        'f_match_adjusted_value_pair_count',
        'default_link_f_match_adjusted_value',
        'b_link_adjusted_value',
        'b_match_adjusted_value_pair_count',
        'default_link_b_match_adjusted_value',
        'construction_link', 
        'flag_construction_matches_pair_count',
        'default_link_flag_construction_matches',
        'constrain_marker' # these not requested but usefull
        ]

    # not part of the pipeline the below
    post_win_df['total weight (A*G*O)'] = post_win_df['design_weight']*post_win_df['calibration_factor']*post_win_df['outlier_weight']
    
    post_win_df['weighted adjusted value'] =post_win_df['adjusted_value'] * post_win_df['total weight (A*G*O)']


    return post_win_df[requested_columns]

def load_config():
        with open('test_outputs_config.json') as f:
            return json.load(f)
    

def join_l_values(df,l_values_path, classification_values_path): 
    """Read l values, classifications and drop duplicates and period"""
    
    l_values = pd.read_csv(l_values_path)
    
   # l_values = l_values.drop_duplicates(['question_no','classification'])
    
   # l_values = l_values.drop(columns=["period"])

    ## Merge on classification SIC map (merge on SIC to get classsificaion on df -> )
    classification_values = pd.read_csv(classification_values_path)
    
    print(list(classification_values))
    df = pd.merge(df, classification_values, left_on="frosic2007", right_on="sic_5_digit", how="left")
    # left on question frocsic .-> Change to left on question_no and classication from above 
    df = pd.merge(df,l_values,how="left",left_on=["question_no","classification"],right_on=["question_no","classification"])

    return df


def extract_mannual_constructed(df):
    """Get data which were mannual constructed"""
    
    df_man = df.loc[(df["type"]==5)]
    
    df_man = df_man[['period', 'reference', 'question_no','adjusted_value']]
    
    return df_man
        
