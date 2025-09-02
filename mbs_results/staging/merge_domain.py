import pandas as pd


def merge_domain(
    input_df: pd.DataFrame,
    domain_mapping: pd.DataFrame,
    sic_input: str,
    sic_mapping: str,
) -> pd.DataFrame:
    """
    Returning a merged dataframe including SIC and domain from the domain mapping
    dataframe.

    Parameters
    ----------
    input_df : pd.DataFrame
        Dataframe containing reference, imp_class, period and SIC columns
    domain_mapping : pd.DataFrame
        Dataframe containing SIC and domain columns
    sic_input : str
        name of column in input_df dataframe containing SIC variable
    sic_mapping : str
        name of column in domain_mapping dataframe containing SIC variable

    Returns
    -------
    pd.DataFrame
        dataframe with SIC and domain columns merged.

    """
    # figure out why this is needed after changes to additional output df func
    merged_df = input_df.drop(columns=[sic_mapping], errors="ignore").merge(
        domain_mapping, how="left", left_on=sic_input, right_on=sic_mapping
    )

    return merged_df.drop(columns=sic_mapping)
