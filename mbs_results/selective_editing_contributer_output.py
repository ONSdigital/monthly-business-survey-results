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
    survey_code is fixed at 009 for MBS

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

    Examples
    --------
    >> get_selective_editing_contributer_output(
    >>        input_filepath=input_filepath,
    >>        domain_filepath=domain_filepath,
    >>        threshold_filepath=threshold_filepath,
    >>        sic_input="sic_5_digit",
    >>        sic_mapping="sic_5_digit",
    >>        period_selected=202201
    >> )
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

    # Survey code is requested on this output, 009 is MBS code
    selective_editing_contributer_output["survey_code"] = "009"

    return selective_editing_contributer_output.loc[
        selective_editing_contributer_output["period"] == period_selected
    ]
