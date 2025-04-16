import numpy as np
import pandas as pd


def get_growth_rates_output(
    additional_outputs_df: pd.DataFrame, **config
) -> pd.DataFrame:
    """
    Time series of weighted adjusted values by classification, question number,
    and cell number.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        dataframe containing classification, question code, cell number,
        period, and weighted adjusted value.
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        Dataframe containing classification, question number, and cell number, pivoted
        wider on period with adjusted values.
    """

    input_data = additional_outputs_df[
        [
            "classification",
            config["question_no"],
            config["cell_number"],
            config["period"],
            config["target"],
        ]
    ]

    input_data["sizeband"] = np.where(
        input_data[config["cell_number"]].isna(),
        input_data[config["cell_number"]],
        input_data[config["cell_number"]].astype(str).str[-1],
    ).astype(int)

    input_data.drop(columns=[config["cell_number"]], inplace=True)

    input_data.sort_values(
        ["classification", config["question_no"], "sizeband", config["period"]],
        inplace=True,
    )

    growth_rates_output = (
        input_data.pivot_table(
            columns=config["period"],
            values=config["target"],
            index=["classification", config["question_no"], "sizeband"],
            aggfunc="sum",
            dropna=False,
        )
        .reset_index()
        .fillna(0)
        .rename_axis(None, axis=1)
    )

    growth_rates_output.rename(
        columns={
            col: pd.to_datetime(col, format = "%Y%m").strftime("%Y%b").upper()
            for col in growth_rates_output.columns[3:]
        },
        inplace=True,
    )

    return growth_rates_output
