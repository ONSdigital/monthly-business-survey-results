import glob

import pandas as pd


def csw_to_spp(filepath):

    files = glob.glob(filepath + "qv*") + glob.glob(filepath + "cp*")

    li = []

    for f in files:

        temp_df = pd.read_csv(f)

        li.append(temp_df)

        print(f"Successfully created dataframe for {f} with shape {temp_df.shape}")
