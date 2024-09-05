import fnmatch
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
import glob

import json

	
# pip install git+https://github.com/ONSdigital/monthly-business-survey-results.git
from mbs_results.utils import read_colon_separated_file, convert_column_to_datetime


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
        "frotover","form_type","response_type","type","error_mkr","froempees"]]
    
    # Updating response_type to 1 for 15399057545 , this is a known error check ASAP-492 for details
    df.loc[df['reference']==15399057545,"response_type"] = 1
    
    zero_to_null_rules = (
         ((df["adjusted_value"]==0) 
         & ((df["response_type"]>=4)  #response_type >=4 leave 0 
        | ((df["response_type"]==2) & (df["type"]==1)) # or response_type 2 and type 1 leave 0
            )) & (df["period"]!=df["period"].min()) # Avoiding overwriting first period records as this will cause some business not to forward impute
        )
    
    convert_to_null_rules = (
        ((df["response_type"]==1) & (df["type"]==5)) #response_type 1 type 5 remove values
        )
    
    remove_values_rules = (
        (df["type"]==2) # drop type 2    
        )
    
    df.drop(df[remove_values_rules].index, inplace=True)
    
    df.loc[zero_to_null_rules,"adjusted_value"] = np.nan
    df.loc[convert_to_null_rules,"adjusted_value"] = np.nan

    df.reset_index(drop=True,inplace=True)

    #Some checks to check if rules were applied
    print("The below must not have values for type 2 since we dropped them\n",
          df.groupby(["response_type","type"])["adjusted_value"].count()
          )
    
    print("Selecting all the nas and zeros of adjusted value\n",
        "The below must not have 0 for response_type 2 type 1 and response_type >=4\n",
        "Also for response type 1 and type 5 must have only nans (mannual costructed)\n",
        df.loc[df["adjusted_value"].isin([np.nan,0])].groupby(["response_type","type"])["adjusted_value"].unique())
    
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
        'reference',"period","frosic2007",'classification','cell_no','frotover',
        'froempees', 'form_type', 'response_type', 'question_no','adjusted_value',
        'error_mkr', 'design_weight','calibration_factor', 'outlier_weight',
        'total weight (A*G*O)','weighted adjusted value', 'imputation_flags_adjusted_value',
        'imp_class','f_link_adjusted_value', 'f_match_adjusted_value_pair_count',
        'default_link_f_match_adjusted_value','b_link_adjusted_value',
        'b_match_adjusted_value_pair_count','default_link_b_match_adjusted_value',
        'construction_link', 'flag_construction_matches_pair_count','default_link_flag_construction_matches',
        "imputed_value","constrain_marker" # these not requested but usefull
        ]

    # not part of the pipeline the below
    post_win_df['total weight (A*G*O)'] = post_win_df['design_weight']*post_win_df['calibration_factor']*post_win_df['outlier_weight']
    
    post_win_df['weighted adjusted value'] =post_win_df['imputed_value'] * post_win_df['total weight (A*G*O)']


    return post_win_df[requested_columns]

def load_config():
        with open('test_outputs_config.json') as f:
            return json.load(f)
    

def join_l_values(df,l_values_path): 
    """Read l values and drop duplicates and period"""
    
    l_values = pd.read_csv(l_values_path)
    
    l_values = l_values.drop_duplicates(['question_no','classification'])
    
    l_values = l_values.drop(columns=["period"])
    
    df['frosic2007_3d'] = (pd.Series(np.floor(df['frosic2007']/100 )*100)).astype(int)
    
    df = pd.merge(df,l_values,how="left",left_on=["question_no","frosic2007_3d"],right_on=["question_no","classification"])

    return df


def extract_mannual_constructed(df):
    """Get data which were mannual constructed"""
    
    df_man = df.loc[((df["response_type"]==1) & (df["type"]==5))]
    
    df_man = df_man[['period', 'reference', 'question_no','adjusted_value']]
    
    return df_man
        
    


