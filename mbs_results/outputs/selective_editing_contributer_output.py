import pandas as pd

from mbs_results.staging.merge_domain import merge_domain


def get_selective_editing_contributer_output(
    additional_outputs_df: pd.DataFrame,
    sic_domain_mapping_path: str,
    threshold_filepath: str,
    period_selected: int,
    **config
) -> pd.DataFrame:
    """
    Returns a dataframe containing period, reference, domain_group, and
    design_weight.
    survey_code is fixed at 009 for MBS

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        Dataframe containing reference, design_weight, formtype, period and SIC columns.
    sic_domain_mapping_path : str
        Filepath to csv file containing SIC and domain columns.
    threshold_filepath : str
        Filepath to csv file containing form type, domain and threshold columns.
    period_selected : int
        period to include in outputs
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary

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
    >>        period_selected=202201
    >> )
    """

    input_data = additional_outputs_df[
        [
            "period",
            "reference",
            "design_weight",
            "frosic2007",
            "formtype",
            "construction_link",
        ]
    ]

    domain_data = pd.read_csv(sic_domain_mapping_path).astype(str)

    threshold_mapping = pd.read_csv(threshold_filepath).astype(str)

    selective_editing_contributer_output = merge_domain(
        input_data, domain_data, "frosic2007", "sic_5_digit"
    )

    selective_editing_contributer_output = pd.merge(
        selective_editing_contributer_output,
        threshold_mapping,
        left_on=["formtype", "domain"],
        right_on=["form", "domain"],
        how="left",
    ).drop(columns=["form", "formtype"])

    selective_editing_contributer_output = selective_editing_contributer_output.rename(
        columns={"reference": "ruref", "domain": "domain_group"}
    )

    # Survey code is requested on this output, 009 is MBS code
    selective_editing_contributer_output["survey_code"] = "009"

    return selective_editing_contributer_output.loc[
        selective_editing_contributer_output["period"] == period_selected
    ]
