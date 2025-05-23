import pandas as pd


def derive_sic_domain_mapping(
    classification_sic_mapping_path: str,
    classification_domain_mapping_path: str,
    output_path: str,
):
    """
    Run function to create a new SIC to domain mapping file, this is then saved in
    the output_path.

    Parameters
    ----------
    classification_sic_mapping_path : str
        path to classification_sic_mapping csv
    classification_domain_mapping_path : str
        path to classification_domain_mapping csv
    output_path : str
        path to save the created sic_domain_mapping.csv

    Examples
    --------
    >>    sharepoint_path = ""
    >>    classification_sic_mapping_path = sharepoint_path +
    >>                                      "classification_sic_mapping.csv"
    >>    classification_domain_mapping_path = (sharepoint_path +
    >>                                      "classification_domain_mapping.csv")
    >>    output_path = sharepoint_path
    >>    derive_sic_domain_mapping(
    >>            classification_sic_mapping_path,
    >>            classification_domain_mapping_path,
    >>            output_path
    >>    )

    """
    classification_sic_map = pd.read_csv(classification_sic_mapping_path).astype("int")
    class_domain_map = pd.read_csv(classification_domain_mapping_path).astype("int")

    # Using outer join to produce dataframes which have unmatched domains or SICs
    sic_domain_map = pd.merge(
        left=classification_sic_map,
        right=class_domain_map,
        left_on="classification",
        right_on="classification",
        how="outer",
    )

    unmatched_sic = (
        sic_domain_map.loc[
            (sic_domain_map["sic_5_digit"].notna()) & (sic_domain_map["domain"].isna())
        ]
        .drop(columns="domain")
        .astype("int")
    )
    unmatched_domain = (
        sic_domain_map.loc[
            (sic_domain_map["sic_5_digit"].isna()) & (sic_domain_map["domain"].notna())
        ]
        .drop(columns="sic_5_digit")
        .astype("int")
    )
    sic_domain_map = (
        sic_domain_map.drop(columns="classification").dropna().astype("int")
    )

    # Outputting matched and unmatched files to sharepoint
    sic_domain_map.to_csv(output_path + "sic_domain_mapping.csv", index=False)
    unmatched_sic.to_csv(output_path + "sic_domain_unmatched_sic.csv", index=False)
    unmatched_domain.to_csv(
        output_path + "sic_domain_unmatched_domain.csv", index=False
    )
