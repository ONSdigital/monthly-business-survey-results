import pandas as pd

from mbs_results.merge_domain import merge_domain
from mbs_results.unsorted.selective_editing import (
    calculate_predicted_value,
    create_standardising_factor,
)


def create_selective_editing_question_output(
    df: pd.DataFrame,
    reference: str,
    period: str,
    domain: str,
    question_no: str,
    sic: str,
    aux: str,
    a_weight: str,
    o_weight: str,
    g_weight: str,
    imputed_value: str,
    adjusted_value: str,
    sic_domain_mapping_path: str,
    period_selected: int,
) -> pd.DataFrame:
    """
     creates the selective editing question output.
     survey_code is fixed at 009 for MBS

     Parameters
     ----------
     df : pd.DataFrame
         Reference dataframe with domain, a_weights, o_weights, and g_weights
     reference : str
         name of column in dataframe containing reference variable
     period : str
         name of column in dataframe containing period variable
     domain : str
         name of column name containing domain variable in sic_domain_mapping file.
     question_no : str
         name of column in dataframe containing question number variable
     sic : str
         name of column in dataframe containing sic variable
     aux : str
         name of column in dataframe containing auxiliary value variable
     a_weight : str
         Column name containing the design weight.
     o_weight : str
         column name containing the outlier weight.
     g_weight : str
         column name containing the g weight.
     imputed_value : str
         name of column in dataframe containing imputed_value variable
     adjusted_value : str
         name of column in dataframe containing adjusted_value variable
    sic_domain_mapping_path : str
         path to the sic domain mapping file
     period_selected : int
         previous period to take the weights for estimation of standardising factor in
         the format yyyymm

     Returns
     -------
     pd.DataFrame
         dataframe formatted for the SPP selective editing output question

     Examples
     --------
     >> output = create_selective_editing_question_output(
     >>            df=wins_output,
     >>            reference="reference",
     >>            period="period",
     >>            domain="domain",
     >>            question_no="question_no",
     >>            sic="sic_5_digit",
     >>            aux="frotover",
     >>            a_weight="design_weight",
     >>            o_weight="outlier_weight",
     >>            g_weight="calibration_factor",
     >>            imputed_value="imputed_value",
     >>            adjusted_value="adjusted_value",
     >>            sic_domain_mapping_path="mapping_files/sic_domain_mapping.csv",
     >>            period_selected=previous_period,
     >>            )
    """
    sic_domain_mapping = pd.read_csv(sic_domain_mapping_path).astype(int)

    df_with_domain = merge_domain(
        input_df=df,
        domain_mapping=sic_domain_mapping,
        sic_input=sic,
        sic_mapping="sic_5_digit",
    )

    df_with_domain = calculate_predicted_value(
        dataframe=df_with_domain,
        imputed_value=imputed_value,
        adjusted_value=adjusted_value,
    )

    standardising_factor = create_standardising_factor(
        dataframe=df_with_domain,
        reference=reference,
        period=period,
        domain=domain,
        question_no=question_no,
        predicted_value="predicted_value",
        imputation_marker="imputation_flags_adjusted_value",
        a_weight=a_weight,
        o_weight=o_weight,
        g_weight=g_weight,
        auxiliary_value=aux,
        period_selected=period_selected,
    )

    # Survey code is requested on this output, 009 is MBS code
    standardising_factor["survey_code"] = "009"

    standardising_factor["imputation_flags_adjusted_value"] = standardising_factor[
        "imputation_flags_adjusted_value"
    ].str.upper()
    standardising_factor = standardising_factor.rename(
        columns={
            "reference": "ruref",
            "domain": "domain_group",
            aux: "auxiliary_value",
            "imputation_flags_adjusted_value": "imputation_marker",
            question_no: "question_code",
        }
    )

    return standardising_factor


def validation_checks_selective_editing(df: pd.DataFrame):
    """
    validation checks for duplicate rows and number of NaNs
    Currently prints to console.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe output from `create_selective_editing_question_output`
    """
    number_dupes = df.duplicated(subset=["period", "question_code", "ruref"]).sum()
    print(
        "Number of duplicates, (checking period, question_no, and reference:",
        number_dupes,
    )
    if number_dupes != 0:
        duped_ids = df.loc[
            df.duplicated(subset=["period", "question_code", "ruref"]), "ruref"
        ]
        print(df.loc[df["ruref"].isin(duped_ids.to_list())])
    predicted_na = df.loc[df["predicted_value"].isna()]
    number_nans = predicted_na.count()[0]
    print(f"predicted_values has {number_nans} NaNs")
