import numpy as np
import pandas as pd

from mbs_results.staging.merge_domain import merge_domain
from mbs_results.utilities.utils import convert_column_to_datetime


def get_weighted_adj_val_time_series(
    additional_outputs_df: pd.DataFrame, sic_class_mapping: str, **config
) -> pd.DataFrame:
    """
    Time series of weighted adjusted values by classification, question number,
    and cell number.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        dataframe containing classification, question code, cell number,
        period, and weighted adjusted value.
    sic_class_mapping : str
        filepath to mapping between SIC and classification
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        Dataframe containing classification, question number, and cell number, pivoted
        wider on period with adjusted values.
    """

    additional_outputs_df = merge_domain(
        additional_outputs_df, sic_class_mapping, "frosic2007", "sic_5_digit"
    )

    # TODO: Find out calculation of weighted adjusted value and derive as necessary
    # in function
    input_data = additional_outputs_df[
        [
            "classification",
            "questioncode",
            "cell_no",
            "period",
            "weighted adjusted value",
        ]
    ].astype(
        {
            "classification": "Int32",
            "question_no": "Int8",
            "cell_no": "Int16",
            "period": "Int32",
            "weighted adjusted value": "float64",
        }
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

    weighted_adj_val_time_series = (
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

    return weighted_adj_val_time_series
