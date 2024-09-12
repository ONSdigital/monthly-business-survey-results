import numpy as np
import pandas as pd
from utils import convert_column_to_datetime


def get_weighted_adj_val_time_series(filepath: str) -> pd.DataFrame:
    """
    Time series of weighted adjusted values by classification, question number,
    and cell number.

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
            "weighted adjusted value",
        ],
        dtype={
            "classification": "Int32",
            "question_no": "Int8",
            "cell_no": "Int16",
            "period": "Int32",
            "weighted adjusted value": "float64",
        },
    )

    input_data["period"] = (
        convert_column_to_datetime(input_data["period"]).dt.strftime("%Y%b").str.upper()
    )

    input_data["sizeband"] = np.where(
        input_data["cell_no"].isna(),
        input_data["cell_no"],
        input_data.cell_no.astype(str).str[-1],
    )

    input_data.drop(columns=["cell_no"], inplace=True)

    input_data.sort_values(
        ["classification", "question_no", "sizeband", "period"], inplace=True
    )

    growth_rate_output = (
        input_data.pivot_table(
            columns="period",
            values="weighted adjusted value",
            index=["classification", "question_no", "sizeband"],
            aggfunc="sum",
            dropna=False,
        )
        .reset_index()
        .dropna(how="any")
    )

    return growth_rate_output
