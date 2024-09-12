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
            "cell_no_x",
            "period_x",
            "weighted_adjusted_value",
        ],
        dtype={
            "classification": "Int32",
            "question_no": "Int8",
            "cell_no_x": "Int16",
            "period_x": "Int32",
            "weighted_adjusted_value": "float64",
        },
    )

    input_data["period_x"] = (
        convert_column_to_datetime(input_data["period_x"])
        .dt.strftime("%Y%b")
        .str.upper()
    )

    input_data["sizeband"] = np.where(
        input_data["cell_no_x"].isna(),
        input_data["cell_no_x"],
        input_data.cell_no_x.astype(str).str[-1],
    )

    input_data.drop(columns=["cell_no_x"], inplace=True)

    input_data.sort_values(
        ["classification", "question_no", "sizeband", "period_x"], inplace=True
    )

    growth_rate_output = (
        input_data.pivot_table(
            columns="period_x",
            values="adjusted_value",
            index=["classification", "question_no", "sizeband"],
            aggfunc="sum",
            dropna=False,
        )
        .reset_index()
        .dropna(how="any")
    )

    return growth_rate_output
