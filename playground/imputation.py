import pandas as pd
import numpy as np
import os

data_folder = os.getcwd() + "/monthly-business-survey-results/playground/data/"
file_path = data_folder + "dummy_imputation_data.csv"
# method 0 - masking

df = pd.read_csv(file_path)
df["period"] = pd.to_datetime(df["period"], format = "%Y%m")

def flag_matched_pair(df, forward_or_backward, target, period, reference, strata, time_difference=1):
  """
  function to add flag to df if data forms a matched pair
  ----------
  df : pd.DataFrame
      pandas dataframe of original data
  forward_or_backward: str
      either f or b for forward or backward method
  target : str
      column name containing target variable
  period : str
      column name containing time period
  reference : str
      column name containing business reference id
  strata : str
      column name containing strata information (sic)
  time_difference : int
      time difference between predictive and target period in months
  """
        
  if forward_or_backward == "f":
    time_difference = -time_difference    
    
  time_difference = pd.DateOffset(months = time_difference)

  df.set_index([reference, period], inplace=True, drop=False)   
  df[forward_or_backward+"_"+target] = [df.loc[(r, p), target] if (r, p) in df.index else None 
                                        for r, p in zip(df[reference], df[period]+time_difference)]
  df.reset_index(drop=True, inplace=True)

  df[forward_or_backward+"_matched_pair"] = np.where(df[target].notnull() & df[forward_or_backward+"_"+target].notnull(), True, 
      False)

  df[forward_or_backward+"_match_pair_count"] = df.groupby([strata, period])[forward_or_backward+"_matched_pair"].transform("sum")

  return df

x = flag_matched_pair(df, "f", "return", "period", "reference", "group")


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
df["f_imp_links"] = df.groupby("imp_group")["f_imputation_link"].cumprod()

# backwards would look like this, but I haven't done b_imputation_link in this script
#df["b_imp_links"] = df[::-1].groupby("imputation_group")["b_imputation_link"].cumprod()[::-1]


df[['reference', 'period','return','f_imputation_link' ,
       'imp_group', 'f_imp_links']]


# it might be useful to create an imputation class variable
df.sort_values(["group", "reference", "period"], inplace=True)
df['imputation_class'] = ((df["group"].diff(1) != 0) | (df["period"].diff(1) != 1)).astype('int').cumsum()