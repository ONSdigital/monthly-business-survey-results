import numpy as np
import pandas as pd
from utils import convert_column_to_datetime


def get_growth_rate_data(filepath: str) -> pd.DataFrame:
    """
    Filters and pivots wider winsorisation data on period to return growth rate data.

    Parameters
    ----------
    filepath : str
        filepath to the asap output.

    Returns
    -------
    pandas.Data.Frame
        Dataframe containing classification, question number, and cell number, pivoted
        wider on period with adjusted values.
    """

    input_data = pd.read_csv(
        filepath,
        usecols=[
            "classification",
            "question_no",
            "cell_no",
            "period",
            "adjusted_value",
            "total weight (A*G*O)"
        ],
        dtype={
            "classification": "Int32",
            "question_no": "Int8",
            "cell_no": "Int16",
            "period": "Int32",
            "adjusted_value": "float64",
            "total weight (A*G*O)": "float64",
        },
    )
    
    input_data["weighted_adjusted_value"] = input_data["adjusted_value"] * input_data["total weight (A*G*O)"]

    input_data["period"] = (
        convert_column_to_datetime(input_data["period"])
        .dt.strftime("%Y%b")
        .str.upper()
    )

    input_data["sizeband"] = np.where(
        input_data["cell_no"].isna(),
        input_data["cell_no"],
        input_data.cell_no.astype(str).str[-1],
    )

    input_data.drop(columns=["cell_no", "adjusted_value", "total weight (A*G*O)"], inplace=True)

    input_data.sort_values(
        ["classification", "question_no", "sizeband", "period"], inplace=True
    )

    growth_rate_output = (
        input_data.pivot_table(
            columns="period",
            values="weighted_adjusted_value",
            index=["classification", "question_no", "sizeband"],
            aggfunc="sum",
            dropna=False,
        )
        .reset_index()
        .dropna(how="any")
    )

    return growth_rate_output


data = get_growth_rate_data("C:/Users/daviel9/Office for National Statistics/Legacy Uplift - Testing outputs from DAP/MBS/shadow_team/asap_482_df_0.0.2.csv")
data.to_csv("D:/growth_rate_data.csv", index=False)
