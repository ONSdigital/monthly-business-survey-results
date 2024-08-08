import os

import numpy as np
import pandas as pd

path = (
    os.environ["HOME"]
    + "/Office for National Statistics/Legacy Uplift - Testing outputs from DAP/MBS"
)

os.chdir(path)

input_data = pd.read_csv(
    "winsorisation/winsorisation_output_0.0.1.csv",
    usecols=[
        "classification",
        "question_no",
        "cell_no_x",
        "period_x",
        "adjusted_value",
    ],
    dtype={
        "classification": "Int32",
        "question_no": "Int8",
        "cell_no_x": "Int16",
        "period_x": "Int32",
        "adjusted_value": "float64",
    },
)

input_data["period_x"] = (
    pd.to_datetime(input_data["period_x"], format="%Y%m")
    .dt.strftime("%Y%b")
    .str.upper()
)

input_data["sizeband"] = np.where(
    input_data["cell_no_x"].isna(),
    input_data["cell_no_x"],
    input_data.cell_no_x.astype(str).str[-1],
)

input_data.drop(columns=["cell_no_x"], inplace=True)

growth_rate_output = input_data.pivot_table(
    columns="period_x",
    values="adjusted_value",
    index=["classification", "question_no", "sizeband"],
    aggfunc="mean",
    dropna=False,
).reset_index()

print(growth_rate_output.head())
print(growth_rate_output.info())
