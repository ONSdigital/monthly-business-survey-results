import fnmatch
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
import glob
	
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
    
    qv_df['period'] = qv_df[["period","Period"]].agg(sum,axis=1)
    qv_df = qv_df.drop(columns=["Period"])
    
    qv_and_cp = pd.merge(qv_df,cp_df,how = "left",on = ["period","reference"])
    pre_proccessed_df = pd.merge(qv_and_cp,df_finalsel,how = "left",on = ["period","reference"])
    
    return pre_proccessed_df


def proccess_for_pre_impute(df):
    """Makes nan values selects relevant columns and questions no"""
    
    df['period'] = convert_column_to_datetime(df['period'] )
    df.loc[(df["response_type"]==1) & (df['period']!=df['period'].min()),"adjusted_value"] = np.nan
    
    df = df[['period', 'reference', 'question_no', 'adjusted_value',"cell_no","frotover","form_type"]]

    questions = [40, 42, 43, 46, 47, 49, 90, 110]
    
    df = df.loc[df['question_no'].isin(questions)]
    
    df["form_type"] = df["form_type"].str.strip()

    df.drop(df.loc[df['form_type'].isin(["T117G","T167G","T123G","T173G"]) & (df['question_no']==40)].index, inplace=True)
    
    df.drop(df.loc[df['form_type'].isin(["T823G","T873G"]) & (df['question_no']==42)].index, inplace=True)

    df.drop(df.loc[df['form_type'].isin(["T817G","T867G"]) & (df['question_no']==46)].index, inplace=True)

    df.reset_index(drop=True,inplace=True)
    
    return df

    
def check_na_duplicates(df):
    """Does basic checks"""
    
    print("nas: ",df.isna().sum()) #count na
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
