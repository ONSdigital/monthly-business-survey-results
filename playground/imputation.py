import pandas as pd
import numpy as np
import os

data_folder = os.getcwd() + "/monthly-business-survey-results/playground/data/"
file_path = data_folder + "dummy_imputation_data.csv"
# method 0 - masking

df = pd.read_csv(file_path)

df.set_index(["period", "reference"], inplace=True, drop=False)

def get_shift_mask(df, shift = 1):
    """
    later
    """

    return list(zip(df["period"] + shift, df["reference"]))

mask = get_shift_mask(df, shift = 1)

df["prev_return"] = df.loc[mask, "return"] 

df["f_matched_pair"] = np.where(df["return"].notnull() & df["prev_return"].notnull(), True, 
    False)

df["match_pair_count"] = df.groupby(["group", "period"])["f_matched_pair"].transform("sum")



#method 1 - using shift()

df = pd.read_csv(file_path)

shift = 1

df.sort_values(["reference","period"], inplace=True)

df[["prev_r", "prev_p"]] = df.groupby("reference").shift(shift)[["return", "period"]]

df["validate_date"] = np.where(df["period"] - df["prev_p"] == shift, True, False)

df["f_matched_pair"] = np.where(
    df["return"].isnull() | df["prev_r"].isnull() | (df["validate_date"] != True),
    False, 
    True)

df["match_pair_count"] = df.groupby(["group", "period"])["f_matched_pair"].transform("sum")

df

#method 2 - using merge()

df = pd.read_csv(file_path)

delta = 1

df["prev_p"] = df["period"] + delta

df = df.drop("prev_p", axis=1).merge(df[["reference", "prev_p", "return"]],
                                     left_on=["reference", "period"],
                                     right_on=["reference", "prev_p"],
                                     how="left")

df.rename(columns={"return_x":"return", "return_y":"prev_return"}, inplace=True)

df["prev_p"] = df["period"] - delta

df["f_matched_pair"] = np.where(
    df["return"].isnull() | df["prev_return"].isnull(),
    False, 
    True)

df["match_pair_count"] = df.groupby(["group", "period"])["f_matched_pair"].transform("sum")

# imputation link
df["f_link_value_num"] = df["return"] * df["f_matched_pair"] #if the reference is not a matched pair it's link value is 0
df["f_imputation_link_num"] = df.groupby(["group", "period"])["f_link_value_num"].transform("sum") #numerator for imputation link

df["f_link_value_den"] = df["prev_return"] * df["f_matched_pair"]
df["f_imputation_link_den"] = df.groupby(["group", "period"])["f_link_value_den"].transform("sum") #denominator

df["f_imputation_link"] = df["f_imputation_link_num"] / df["f_imputation_link_den"]

# getting an imputation-type flag
# forward fill - same assumption as shift() approach

df["f_imp_flag"] = df.groupby(["group","reference"])["return"].ffill() - df["return"].fillna(0)
df["f_imp_flag"].replace(0, np.nan, inplace=True)
df["f_imp_flag"] = (df["f_imp_flag"] / df["f_imp_flag"]).replace(1, True)

# backward fill - same assumption as shift() approach

df["b_imp_flag"] = df.groupby(["group","reference"])["return"].bfill() - df["return"].fillna(0)
df["b_imp_flag"].replace(0, np.nan, inplace=True)
df["b_imp_flag"] = (df["b_imp_flag"] / df["b_imp_flag"]).replace(1, True)

## want to create an imputation link column for multiple consecutive imputation
# this can be done with a cumulative product on imputation groups
df["missing_value"] = np.where(df["return"].isnull(), True, False)
df['imp_group'] = (df["missing_value"].diff(1) != 0).astype('int').cumsum()
df["f_imp_links"] = df.groupby("imputation_group")["f_imputation_link"].cumprod()

# backwards would look like this, but I haven't done b_imputation_link in this script
#df["b_imp_links"] = df[::-1].groupby("imputation_group")["b_imputation_link"].cumprod()[::-1]


df[['reference', 'period','return','f_imputation_link' ,'imp_group_fill',
       'imputation_group', 'f_imp_links']]
