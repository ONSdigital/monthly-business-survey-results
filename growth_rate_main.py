import os

import pandas as pd

path = (
    os.environ["HOME"]
    + "/Office for National Statistics/Legacy Uplift - Testing outputs from DAP/MBS"
)

os.chdir(path)

input_data = pd.read_csv(
    "winsorisation/winsorisation_output_0.0.1.csv",
    usecols=["classification", "question_no", "cell_no_x", "period", "adjusted_value"],
    dtype={
        "classification": "Int32",
        "question_no": "Int8",
        "cell_no_x": "Int16",
        "period": "Int32",
    },
)

input_data["sizeband"] = input_data.cell_no_x.astype(str).str[-1]
input_data.drop(columns=["cell_no_x"], inplace=True)

pivoted_input_data = input_data.pivot_table(
    columns="period",
    values="adjusted_value",
    index=["classification", "question_no", "sizeband"],
    aggfunc="mean",
    dropna=False,
).reset_index()

print(pivoted_input_data.columns)
print(pivoted_input_data)
