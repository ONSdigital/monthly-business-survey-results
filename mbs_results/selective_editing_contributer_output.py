import pandas as pd

from mbs_results.merge_domain import merge_domain


def get_selective_editing_contributer_output(
    input_filepath: str,
    domain_filepath: str,
    threshold_filepath: str,
    sic_input: str,
    sic_mapping: str,
    period_selected: int,
) -> pd.DataFrame:
    """
        Returns a dataframe containing period, reference, domain_group, and
        design_weight.

        Parameters
        ----------
        input_filepath : str
            Filepath to csv file containing reference, imp_class, period and
            SIC columns.
        domain_filepath : str
            Filepath to csv file containing SIC and domain columns.
        threshold_filepath : str
            Filepath to csv file containing form type, domain and threshold columns.
        sic_input : str
            Name of column in input_filepath csv file containing SIC variable.
        sic_mapping : str
            Name of column in domain_filepath csv file containing SIC variable.

        Returns
        -------
        pd.DataFrame
            Dataframe with SIC and domain columns merged.
    `
    """

    input_data = pd.read_csv(
        input_filepath,
        usecols=["period", "reference", "design_weight", sic_input, "form_type"],
    )

    domain_data = pd.read_csv(domain_filepath)

    threshold_mapping = pd.read_csv(threshold_filepath)

    selective_editing_contributer_output = merge_domain(
        input_data, domain_data, sic_input, sic_mapping
    )

    selective_editing_contributer_output = pd.merge(
        selective_editing_contributer_output,
        threshold_mapping,
        left_on=["form_type", "domain"],
        right_on=["form", "domain"],
        how="left",
    ).drop(columns=["form", "form_type"])

    selective_editing_contributer_output = selective_editing_contributer_output.rename(
        columns={"reference": "ruref", "domain": "domain_group"}
    )
    selective_editing_contributer_output["survey_code"] = "009"

    return selective_editing_contributer_output.loc[
        selective_editing_contributer_output["period"] == period_selected
    ]


if __name__ == "__main__":
    from datetime import datetime
    from importlib import metadata

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
