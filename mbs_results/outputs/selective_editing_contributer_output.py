import pandas as pd

from mbs_results.staging.merge_domain import merge_domain


def get_selective_editing_contributer_output(
    additional_outputs_df: pd.DataFrame,
    sic_domain_mapping_path: str,
    threshold_filepath: str,
    period_selected: int,
    question_no: str,
    period: str,
    reference: str,
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
    question_no : str
        Column name containing question number.
    period : str
        Column name containing date information.
    reference : str
        Column name containing reference.
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
    questions_selected = [40, 49]
    input_data = additional_outputs_df.loc[
        additional_outputs_df[question_no].isin(questions_selected)
    ]
    input_data = input_data[
        [period, reference, "design_weight", "frosic2007", "formtype"]
    ]

    input_data["frosic2007"] = input_data["frosic2007"].astype(str)

    domain_data = pd.read_csv(
        sic_domain_mapping_path, dtype={"sic_5_digit": str, "domain": str}
    )
    threshold_mapping = pd.read_csv(
        threshold_filepath, dtype={"formtype": str, "domain": str, "threshold": float}
    )

    selective_editing_contributer_output = merge_domain(
        input_data, domain_data, "frosic2007", "sic_5_digit"
    )

    selective_editing_contributer_output = pd.merge(
        selective_editing_contributer_output,
        threshold_mapping,
        on=["formtype", "domain"],
        how="left",
    ).drop(columns=["formtype"])

    selective_editing_contributer_output = selective_editing_contributer_output.rename(
        columns={"reference": "ruref", "domain": "domain_group"}
    ).drop(columns="frosic2007")

    # Survey code is requested on this output, 009 is MBS code
    selective_editing_contributer_output["survey_code"] = "009"

    return selective_editing_contributer_output.loc[
        selective_editing_contributer_output["period"] == period_selected
    ]
