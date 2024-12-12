import pandas as pd

from mbs_results.outputs.selective_editing import create_standardising_factor
from mbs_results.staging.merge_domain import merge_domain


def create_selective_editing_question_output(
    additional_outputs_df: pd.DataFrame,
    sic_domain_mapping_path: str,
    period_selected: int,
    **config,
) -> pd.DataFrame:
    """
     creates the selective editing question output.
     survey_code is fixed at 009 for MBS

     Parameters
     ----------
     additional_outputs_df : pd.DataFrame
         Reference dataframe with sic, a_weights, o_weights, g_weights,
         adjustedresponse, imputation_flags and frotover
     sic_domain_mapping_path : str
         path to the sic domain mapping file
     period_selected : int
         previous period to take the weights for estimation of standardising factor in
         the format yyyymm
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary

     Returns
     -------
     pd.DataFrame
         dataframe formatted for the SPP selective editing output question

     Examples
     --------
     >> output = create_selective_editing_question_output(
     >>            additional_outputs_df=wins_output,
     >>            sic_domain_mapping_path="mapping_files/sic_domain_mapping.csv",
     >>            period_selected=202201,
     >>            )
    """
    sic_domain_mapping = pd.read_csv(sic_domain_mapping_path).astype(str)

    df_with_domain = merge_domain(
        input_df=additional_outputs_df,
        domain_mapping=sic_domain_mapping,
        sic_input="frosic2007",
        sic_mapping="sic_5_digit",
    )

    standardising_factor = create_standardising_factor(
        dataframe=df_with_domain,
        reference="reference",
        period="period",
        domain="domain",
        question_no="questioncode",
        predicted_value="adjustedresponse",
        imputation_marker="imputation_flags_adjustedresponse",
        a_weight="design_weight",
        o_weight="outlier_weight",
        g_weight="calibration_factor",
        auxiliary_value="frotover",
        period_selected=period_selected,
    )

    # Survey code is requested on this output, 009 is MBS code
    standardising_factor["survey_code"] = "009"

    standardising_factor["imputation_flags_adjustedresponse"] = standardising_factor[
        "imputation_flags_adjustedresponse"
    ].str.upper()
    standardising_factor = standardising_factor.rename(
        columns={
            "reference": "ruref",
            "domain": "domain_group",
            "frotover": "auxiliary_value",
            "imputation_flags_adjusted_value": "imputation_marker",
            "questioncode": "question_code",
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
