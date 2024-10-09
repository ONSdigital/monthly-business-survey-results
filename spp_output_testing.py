# Todo move calling thsi function into example
from datetime import datetime
from importlib import metadata
from mbs_results.outputs.selective_editing_question_output import create_selective_editing_question_output,validation_checks_selective_editing
from mbs_results.selective_editing_contributer_output import get_selective_editing_contributer_output
import pandas as pd

# question: 

version = metadata.metadata("monthly-business-survey-results")["version"]
output_path = (
    "C:/Users/dayj1/Office for National Statistics/Legacy Uplift - MBS (1)/"
) # mapping_files/sic_domain_mapping.csv
wins_output = pd.read_csv(
    output_path + f"winsorisation/winsorisation_output_{version}.csv"
)
previous_period = 202201
output = create_selective_editing_question_output(
    df=wins_output,
    reference="reference",
    period="period",
    domain="domain",
    question_no="question_no",
    sic="sic_5_digit",
    aux="frotover",
    a_weight="design_weight",
    o_weight="outlier_weight",
    g_weight="calibration_factor",
    imputed_value="imputed_value",
    adjusted_value="adjusted_value",
    sic_domain_mapping_path=output_path+"mapping_files/sic_domain_mapping.csv",
    period_selected=previous_period,
)
validation_checks_selective_editing(output)
formatted_date = datetime.today().strftime("%Y-%m-%d")
output_file_name = (
    f"sopp_mbs_{formatted_date}_selective_editing"
    + f"_question_{previous_period}_{version}.csv"
)
output.to_csv(
    output_path + "selective_editing_outputs/" + output_file_name,
    index=False,
)

number_dupes = wins_output.duplicated(subset=["period", "question_no", "reference"]).sum()
print(
    "Number of duplicates, (checking period, question_no, and reference:",
    number_dupes,
)
if number_dupes != 0:
    duped_ids = wins_output.loc[wins_output.duplicated(subset=["period", "question_no", "reference"]),"reference"]
    print(wins_output.loc[wins_output["reference"].isin(duped_ids.to_list())])

# contributor:

version = metadata.metadata("monthly-business-survey-results")["version"]
output_path = (
    "C:/Users/dayj1/Office for National Statistics/Legacy Uplift - MBS (1)/"
)

input_filepath = output_path + f"winsorisation/winsorisation_output_{version}.csv"
domain_filepath = output_path + "mapping_files/sic_domain_mapping.csv"
threshold_filepath = output_path + "mapping_files/form_domain_threshold_mapping.csv"
period_selected = 202201
output = get_selective_editing_contributer_output(
    input_filepath,
    domain_filepath,
    threshold_filepath,
    "sic_5_digit",
    "sic_5_digit",
    period_selected,
)
formatted_date = datetime.today().strftime("%Y-%m-%d")
output_file_name = (
    f"sopp_mbs_{formatted_date}_selective_editing"
    + f"_contributor_{period_selected}_{version}.csv"
)
output.to_csv(
    output_path + "selective_editing_outputs/" + output_file_name, index=False
)