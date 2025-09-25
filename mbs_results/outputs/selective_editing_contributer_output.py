import pandas as pd

from mbs_results.staging.merge_domain import merge_domain
from mbs_results.utilities.outputs import write_csv_wrapper


def get_selective_editing_contributor_output(
    additional_outputs_df: pd.DataFrame,
    sic_domain_mapping_path: str,
    threshold_filepath: str,
    period_selected: int,
    question_no: str,
    period: str,
    reference: str,
    output_path: str,
    **config,
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
    output_path : str
        path to save output files.
    **config: Dict
          main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        Dataframe with SIC and domain columns merged.

    Examples
    --------
    >> get_selective_editing_contributor_output(
    >>        input_filepath=input_filepath,
    >>        domain_filepath=domain_filepath,
    >>        threshold_filepath=threshold_filepath,
    >>        period_selected=202201
    >> )
    """
    forms_ignored = [
        "203",
        "204",
    ]  # IDBR for water form probably should use spp form type
    input_data = additional_outputs_df.loc[
        ~additional_outputs_df["formtype"].isin(forms_ignored)
    ]
    input_data = input_data[
        [period, reference, "design_weight", config["sic"], "formtype"]
    ]

    input_data[config["sic"]] = input_data[config["sic"]].astype(str)

    domain_data = pd.read_csv(
        sic_domain_mapping_path, dtype={"sic_5_digit": str, "domain": str}
    )
    threshold_mapping = pd.read_csv(
        threshold_filepath, dtype={"formtype": int, "domain": str, "threshold": float}
    )
    # Loading as int to remove leading 0, converting back to str to match main df
    threshold_mapping["formtype"] = threshold_mapping["formtype"].astype(str)

    # Threshold file contains multiple duplicate rows
    threshold_mapping.drop_duplicates(inplace=True)

    selective_editing_contributor_output = merge_domain(
        input_data, domain_data, config["sic"], "sic_5_digit"
    )

    selective_editing_contributor_output = pd.merge(
        selective_editing_contributor_output,
        threshold_mapping,
        on=["formtype", "domain"],
        how="left",
    )
    write_csv_wrapper(
        selective_editing_contributor_output,
        output_path + "se_contributor_full_" + f"se_period_{period_selected}.csv",
        config["platform"],
        config["bucket"],
        index=False,
    )
    selective_editing_contributor_output.drop(columns=["formtype"], inplace=True)

    selective_editing_contributor_output = selective_editing_contributor_output.rename(
        columns={"reference": "ruref", "domain": "domain_group"}
    ).drop(columns=config["sic"])

    # Survey code is requested on this output, 009 is MBS code
    selective_editing_contributor_output["survey_code"] = "009"

    # Dropping duplicates as we expect the same contributor for q40 and q49 in some form
    # types. Selecting only needed period
    contributor_output_without_dupes = selective_editing_contributor_output.loc[
        selective_editing_contributor_output["period"] == period_selected
    ].drop_duplicates()

    return contributor_output_without_dupes
