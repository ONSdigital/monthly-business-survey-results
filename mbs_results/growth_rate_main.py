import numpy as np
import pandas as pd


def get_growth_rate_data(filepath):
    """
    Filters and pivots wider winsorisation data on period to return growth rate data.

    Parameters
    ----------
    filepath : str
        filepath to the winsorisation output.

    Returns
    -------
    pandas.Data.Frame
        Dataframe containing classification, question number, cell number, and pivoted
        wider on period with adjusted values.
    """

    input_data = pd.read_csv(
        filepath,
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

    return growth_rate_output
