from pathlib import Path

import pandas as pd

from mbs_results.utilities.utils import convert_column_to_datetime


def create_csdb_output(
    additional_outputs_df: pd.DataFrame,
    cdid_data_path: str,
    **config
) -> pd.DataFrame:
    """
    creates outputs for CSDB, only produces monthly aggregations as all higher
    aggregations can be derived from these.
    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        estimation input dataframe containing relevant columns for csdb output
    cdid_data_path : str
        path to CDID reference table, this is needed to map classification and question
        number to CDID.
    Returns
    -------
    pd.DataFrame
        pivot table aggregating gross values for each month, values are given in pounds (£)
        thousands. Only returns aggregations of month and not higher periods. Checking
        that output team would be happy with this.
    """

    cdid_mapping = pd.read_csv(cdid_data_path)

    df_combined = pd.merge(
        additional_outputs_df, cdid_mapping, on=["questioncode", "classification"], how="left"
    )
    df_combined["period"] = (
        convert_column_to_datetime(df_combined["period"])
        .dt.strftime("%Y%m")
    )
    # Convert grossed_column into pounds thousands before agg
    df_combined["curr_grossed_value"] = (
        df_combined["adjustedresponse"]
        * df_combined["design_weight"]
        * df_combined["outlier_weight"]
        * df_combined["calibration_factor"]
    ) / 1e3

    df_pivot = pd.pivot_table(
        df_combined,
        values="curr_grossed_value",
        index="cdid",
        columns="period",
        aggfunc=sum,
    )

    return df_pivot
