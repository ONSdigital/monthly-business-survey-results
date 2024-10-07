import pandas as pd

from mbs_results.merge_domain import merge_domain


def get_selective_editing_contributer_output(
    input_filepath: str,
    domain_filepath: str,
    sic_input: str,
    sic_mapping: str,
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
        usecols=[
            "period",
            "reference",
            "design_weight",
            sic_input,
        ],
    )

    domain_data = pd.read_csv(domain_filepath)

    selective_editing_contributer_output = merge_domain(
        input_data, domain_data, sic_input, sic_mapping
    )

    selective_editing_contributer_output = selective_editing_contributer_output.rename(
        columns={"reference": "ruref", "domain": "domain_group"}
    )

    return selective_editing_contributer_output
