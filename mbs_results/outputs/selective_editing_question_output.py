import pandas as pd

from mbs_results.outputs.selective_editing import (
    calculate_auxiliary_value,
    create_standardising_factor,
)
from mbs_results.staging.merge_domain import merge_domain
from mbs_results.utilities.inputs import read_csv_wrapper
from mbs_results.utilities.outputs import save_df


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
          Main pipeline configuration. Used for SIC

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
    questions_selected = [40, 49]
    input_data = additional_outputs_df.loc[
        additional_outputs_df["questioncode"].isin(questions_selected)
    ]
    input_data = input_data[(input_data["period"] == period_selected)]

    sic_domain_mapping = read_csv_wrapper(
        sic_domain_mapping_path, config["platform"], config["bucket"]
    ).astype(str)

    df_with_domain = merge_domain(
        input_df=input_data,
        domain_mapping=sic_domain_mapping,
        sic_input=config["sic"],
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
        imputation_class="imputation_class",
        a_weight="design_weight",
        o_weight="outlier_weight",
        g_weight="calibration_factor",
        period_selected=period_selected,
    )

    auxiliary_value = calculate_auxiliary_value(
        dataframe=input_data,
        reference="reference",
        period="period",
        question_no="questioncode",
        frozen_turnover="converted_frotover",
        construction_link="construction_link",
        imputation_class="imputation_class",
        period_selected=period_selected,
    )

    question_output = pd.merge(
        standardising_factor,
        auxiliary_value,
        on=["period", "reference", "imputation_class", "questioncode"],
        how="left",
    )

    # Survey code is required on this output, 009 is MBS code
    question_output["survey_code"] = "009"

    question_output["imputation_flags_adjustedresponse"] = question_output[
        "imputation_flags_adjustedresponse"
    ].str.upper()

    question_output = question_output.rename(
        columns={
            "reference": "ruref",
            "domain": "domain_group",
            "frotover": "auxiliary_value",
            "imputation_flags_adjustedresponse": "imputation_marker",
            "adjustedresponse": "predicted_value",
            "questioncode": "question_code",
        }
    )

    question_output.fillna({"auxiliary_value": 0}, inplace=True)
    save_df(
        question_output,
        f"selective_editing_question_full_output_period_{period_selected}.csv",
        config,
        config["debug_mode"],
    )

    question_output.drop(
        columns=[
            "imputation_class",
            "construction_link",
            "converted_frotover",
            "formtype",
            "design_weight",
            "outlier_weight",
            "calibration_factor",
        ],
        axis=1,
        inplace=True,
    )

    return question_output


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
