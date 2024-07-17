import pandas as pd


def merge_domain(
    input_df: pd.DataFrame,
    domain_mapping: pd.DataFrame,
    SIC_input: str,
    SIC_mapping: str,
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
    SIC_input : str
        name of column in input_df dataframe containing SIC variable
    SIC_mapping : str
        name of column in domain_mapping dataframe containing SIC variable

    Returns
    -------
    pd.DataFrame
        dataframe with SIC and domain columns merged.

    """

    merged_df = input_df.merge(
        domain_mapping, how="left", left_on=SIC_input, right_on=SIC_mapping
    )

    return merged_df.drop(columns=SIC_mapping)
