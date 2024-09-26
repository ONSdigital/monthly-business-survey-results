from pathlib import Path

import pandas as pd

from mbs_results.utils import convert_column_to_datetime


def create_csdb_output(
    df: pd.DataFrame,
    gross_column: str,
    question_no: str,
    classification: str,
    period: str,
    cdid_data_path: str,
) -> pd.DataFrame:
    """
    creates outputs for CSDB, only produces monthly aggregations as all higher
    aggregations can be derived from these.

    Parameters
    ----------
    df : pd.DataFrame
        post winsorised dataframe with grossed value column (a*g*o*value)
    gross_column : str
        grossed value column name
    question_no : str
        question number column name
    classification : str
        classification column name
    period : str
        period column name
    cdid_data_path : str
        path to CDID reference table, this is needed to map classification and question
        number to CDID.

    Returns
    -------
    pd.DataFrame
        pivot table aggregating gross values for each month, values are given in million
        pounds (£). Only returns aggregations of month and not higher periods. Checking
        that output team would be happy with this.
    """
    cdid_mapping = pd.read_csv(cdid_data_path)
    cdid_mapping.rename(
        columns={"SIC on CS": classification, "Question Number on CS": question_no},
        inplace=True,
    )
    cdid_mapping = cdid_mapping[["cdid", question_no, classification]]

    df_combined = pd.merge(
        df, cdid_mapping, on=[question_no, classification], how="left"
    )
    df_combined["period"] = (
        convert_column_to_datetime(df_combined["period"])
        .dt.strftime("%Y%b")
        .str.lower()
    )
    # Convert grossed_column into million £'s before agg
    df_combined[gross_column] = df_combined[gross_column] / 1e6

    df_pivot = pd.pivot_table(
        df_combined,
        values=gross_column,
        index="period",
        columns="cdid",
        aggfunc=sum,
    )

    return df_pivot


if __name__ == "__main__":
    cdid_data_path = Path(r"D:\tmp\csdb_raw_data_metadata.csv")
    data_path = Path(r"D:\temp_outputs_new_env")
    df = pd.read_csv(data_path / "winsorisation_output_0.0.2.csv")
    df["period"] = convert_column_to_datetime(df["period"])
    output = create_csdb_output(
        df,
        gross_column="new_target_variable",
        question_no="question_no",
        classification="classification",
        period="period",
        cdid_data_path=cdid_data_path,
    )
