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
df["f_imp_flag"] = (df["f_imp_flag"] / df["f_imp_flag"]).replace(1, "FIR")

# backward fill - same assumption as shift() approach

df["b_imp_flag"] = df.groupby(["group","reference"])["return"].bfill() - df["return"].fillna(0)
df["b_imp_flag"].replace(0, np.nan, inplace=True)
df["b_imp_flag"] = (df["b_imp_flag"] / df["b_imp_flag"]).replace(1, "BIR")

# would finding the min date with return help? or define a grouping that has a return then missing values
df["period_with_return"] = df["period"][df["return"].notnull()]
df["earliest_return"] = df.groupby(["group", "reference"])["period_with_return"].transform("min")

## want to create an imputation link column for multiple consecutive imputation
# this can be done with a cumulative sum

df[["reference", "f_imputation_link"]]
df.groupby("reference").apply(lambda x: x["f_imputation_link"].cumsum())

df.groupby("reference").apply(lambda x: np.where(x["return"].isnull(), x["f_imputation_link"].cumsum(), np.nan))
