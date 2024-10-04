from importlib import metadata

import pandas as pd

from mbs_results.merge_domain import merge_domain
from mbs_results.unsorted.selective_editing import (
    calculate_predicted_value,
    create_standardising_factor,
)


def create_selective_editing_question_output(
    df: pd.DataFrame, output_path: str, version: str, previous_period: int
):
    sic_domain_mapping = pd.read_csv(
        output_path + "mapping_files/sic_domain_mapping.csv"
    ).astype(int)
    df_with_domain = merge_domain(df, sic_domain_mapping, "sic_5_digit", "sic_5_digit")
    df_with_domain = calculate_predicted_value(
        df_with_domain, "imputed_value", "adjusted_value"
    )
    standardising_factor = create_standardising_factor(
        dataframe=df_with_domain,
        reference="reference",
        period="period",
        domain="domain",
        question_no="question_no",
        predicted_value="predicted_value",
        imputation_marker="imputation_flags_adjusted_value",
        a_weight="design_weight",  # a_weight?
        o_weight="outlier_weight",
        g_weight="calibration_factor",  # g_weight?
        auxiliary_value="frotover",
        previous_period=previous_period,
    )
    standardising_factor["survey_code"] = 23
    standardising_factor.to_csv(
        output_path
        + "selective_editing_outputs/"
        + f"selective_editing_question_output_{previous_period}_{version}.csv",
        index=False,
    )


if __name__ == "__main__":
    version = metadata.metadata("monthly-business-survey-results")["version"]
    output_path = ""
    wins_output = pd.read_csv(
        output_path + f"winsorisation/winsorisation_output_{version}.csv"
    )
    create_selective_editing_question_output(
        df=wins_output, output_path=output_path, version=version, previous_period=202201
    )
