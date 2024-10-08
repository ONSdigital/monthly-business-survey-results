from importlib import metadata

import pandas as pd

from mbs_results.merge_domain import merge_domain
from mbs_results.unsorted.selective_editing import (
    calculate_predicted_value,
    create_standardising_factor,
)


def create_selective_editing_question_output(
    df: pd.DataFrame,
    reference ,
    period,
    domain,
    question_no,
    sic ,
    aux: str,
    a_weight: str,
    o_weight: str,
    g_weight: str,
    predicted_value: str,
    imputed_value ,
    adjusted_value: str,
    output_path: str, 
    version: str, 
    previous_period: int
):
    """
    creates and saves the selective editing question output to the output_path folder
    saves it as a csv with the same version tag

    Parameters
    ----------
    df : pd.DataFrame
        input data, this uses the output produced by the winsorisation functions
    output_path : str
        path to save the output csv. Output file path must be the sharepoint as this
        path is also used to load needed mapping files (could change to individual arg?)
    version : str
        version of the monthly-business-survey-results package, used to tag the output
        with the correct version of the release
    previous_period : int
        previous period to take the weights for estimation of standardising factor in
        the format yyyymm
    """
    sic_domain_mapping = pd.read_csv(
        output_path + "mapping_files/sic_domain_mapping.csv"
    ).astype(int)
    df_with_domain = merge_domain(input_df=df, 
                                  domain_mapping=sic_domain_mapping, 
                                  sic_input=sic, 
                                  sic_mapping="sic_5_digit")
    df_with_domain = calculate_predicted_value(
        dataframe=df_with_domain, imputed_value=imputed_value, adjusted_value=adjusted_value
    )
    standardising_factor = create_standardising_factor(
        dataframe = df_with_domain,
        reference=reference,
        period=period,
        domain=domain,
        question_no=question_no,
        predicted_value=predicted_value,
        imputation_marker="imputation_flags_adjusted_value",
        a_weight=a_weight,
        o_weight=o_weight,
        g_weight=g_weight,
        auxiliary_value=aux,
        previous_period=previous_period,
    )
    standardising_factor["survey_code"] = "009"
    save_output = False
    if save_output:
        standardising_factor.to_csv(
            output_path
            + "selective_editing_outputs/"
            + f"selective_editing_question_output_{previous_period}_{version}.csv",
            index=False,
        )
    return standardising_factor


if __name__ == "__main__":

    version = metadata.metadata("monthly-business-survey-results")["version"]
    output_path = (
        "C:/Users/dayj1/Office for National Statistics/Legacy Uplift - MBS (1)/"
    )
    wins_output = pd.read_csv(
        output_path + f"winsorisation/winsorisation_output_{version}.csv"
    )


    out = create_selective_editing_question_output(df=wins_output,
                                                   reference="reference",
                                                   period="period",
                                                   domain="domain", 
                                                   question_no="question_no",
                                                   sic = "sic_5_digit", 
                                                   aux="frotover",
                                                   a_weight="design_weight",
                                                   o_weight = "outlier_weight",
                                                   g_weight = "calibration_factor",
                                                   predicted_value= "predicted_value",
                                                   imputed_value="imputed_value", 
                                                   adjusted_value="adjusted_value", 
                                                   output_path=output_path,
                                                   version=version,
                                                   previous_period=202302)

    print(out)

    # print('Number of duplicates, (checking period, question_no, and reference:',
    #        saved_version.duplicated(subset=['period',"question_no","reference"]).sum())
    # predicted_na = saved_version.loc[saved_version["predicted_value"].isna()]
    # number_nans = predicted_na.count()[0]
    # reference_no_list = predicted_na["reference"].unique().tolist()
    # print(f"predicted_values has {number_nans} NaNs")
    # print(wins_output.loc[(wins_output["reference"].isin(reference_no_list))
    #                       & (wins_output["question_no"] == 40)
    #                       & (wins_output["period"] == 202302)])

    # df = df_with_domain,
    #     reference=reference,
    #     period=period,
    #     domain=domain,
    #     question_no=question_no,
    #     predicted_value="predicted_value",
    #     imputation_marker="imputation_flags_adjusted_value",
    #     a_weight="design_weight",
    #     o_weight="outlier_weight",
    #     g_weight="calibration_factor",
    #     auxiliary_value="frotover",
    #     previous_period=previous_period,
