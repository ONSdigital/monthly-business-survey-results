import os

import pandas as pd

path = (
    os.environ["HOME"]
    + "/Office for National Statistics/Legacy Uplift - Testing outputs from DAP/MBS"
)
print(path)
os.chdir(path)

input_data = pd.read_csv("winsorisation/winsorisation_output_0.0.1.csv")
