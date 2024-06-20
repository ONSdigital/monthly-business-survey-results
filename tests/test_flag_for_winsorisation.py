""" What we need

dataframe
a weight
g weight

a * g to create a new column

then flag anything <=1 as not to be winsorised

"""

import pandas as pd


def winsorisation_flag(df, a_weight, g_weight):

    df["new_col"] = df.a_weight * df.g_weight

    df["NW_AG_flag"] = df["new_col"].apply(lambda x: "NW_AG" if x <= 1 else "")

    return df


data = pd.read_csv(
    "/home/cdsw/monthly-business-survey-results/tests/data/winsorisation/flag_data.csv"
)

print(data)

test = winsorisation_flag(data, data.a_weight, data.g_weight)
