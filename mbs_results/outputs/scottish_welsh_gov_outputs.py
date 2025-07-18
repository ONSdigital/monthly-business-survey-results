import numpy as np
import pandas as pd

from mbs_results import logger
from mbs_results.utilities.inputs import read_colon_separated_file

# Missing reporting unit RU and name
# Should SIC be frozenSIC or SIC)5_digit
# Missing, name (LU), enterprise group, status? start and end date questions removed?
# RU - reference (RU REF)
# Waiting on Local Unit data as well?


def split_func(my_string: str) -> str:
    """
    splitting strings used to sort columns when two levels of columns
    are combined. Selects the final part of string to sort by

    Parameters
    ----------
    my_string : str
        any string with underscores, typically column names

    Returns
    -------
    str
        final part of string once separated on underscores
    """
    return my_string.split(sep=("_"))[-1]


def filter_and_calculate_percent_devolved(
    df: pd.DataFrame, local_unit_data: pd.DataFrame, devolved_nation: str
) -> pd.DataFrame:
    """Filter by devolved nation and calculate percentage column"""
    logger.info(f"Calculating percentage for {devolved_nation}")
    devolved_nation = devolved_nation.lower()
    nation_to_code = {"scotland": "XX", "wales": "WW"}
    if devolved_nation not in nation_to_code:
        error_msg = (
            f"Invalid devolved nation '{devolved_nation}'. "
            "Expected 'Scotland' or 'Wales'."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    region_code = nation_to_code[devolved_nation]
    logger.info(f"Filtering for {devolved_nation} with region code {region_code}")
    percent_col = f"percentage_{devolved_nation}"

    # compute the grossed UK turnover or returns
    df["gross_turnover_uk"] = (
        df["adjustedresponse"]
        * df["design_weight"]
        * df["outlier_weight"]
        * df["calibration_factor"]
    )
    # Calculate froempment ratio:
    # (sum of froempment in filtered LU data) / (sum of froempment in df)
    # Filter LU data for the devolved nation region
    region_col = "region"
    froempment_col = "froempment"
    employment_col = "employment"

    # Filter LU data for the devolved nation region and sum employment by reference
    local_unit_data["reference"] = local_unit_data["ruref"]
    regional_employment = (
        local_unit_data[local_unit_data[region_col] == region_code]
        .groupby("reference")[employment_col]
        .sum()
        .reset_index()
        .rename(columns={employment_col: f"{employment_col}_{devolved_nation}"})
    )

    # Sum total employment by reference from the pipeline data
    total_employment = (
        df.groupby("reference")[froempment_col]
        .sum()
        .reset_index()
        .rename(columns={froempment_col: "total_employment"})
    )

    # Merge the two Dataframes and calculate the percentage
    merged_df = pd.merge(
        regional_employment,
        total_employment,
        on="reference",
        how="left",
    )

    merged_df[percent_col] = (
        (
            merged_df[f"{employment_col}_{devolved_nation}"]
            / merged_df["total_employment"]
        )
        * 100
    ).round(2)

    # Add the percentage column to the original DataFrame
    df = df.merge(
        merged_df[["reference", f"percentage_{devolved_nation}"]],
        on="reference",
        how="left",
    )

    return df


def get_devolved_questions(config: dict) -> list:
    """Get devolved questions from config or use default. Integer values expected."""
    return config.get("devolved_questions", [11, 12, 40, 49, 110])


def get_question_no_plaintext(config: dict) -> dict:
    """
    Get question number to plaintext mapping from config or use default.
    Ensures all keys are integers, even if loaded as strings from JSON.
    """
    mapping = config.get("question_no_plaintext")
    if mapping is not None:
        return {int(k): v for k, v in mapping.items()}
    return {
        11: "start_date",
        12: "end_date",
        40: "total_turnover",
        42: "commission_or_fees",
        43: "sales_on_own_account",
        46: "total_from_invoices",
        47: "donations",
        49: "exports",
        110: "water",
    }


def get_lu_cols(config: dict) -> list:
    """Get local unit columns from config or use default."""
    return config.get(
        "local_unit_columns",
        [
            "ruref",
            "entref",
            "lu ref",
            "check letter",
            "sic03",
            "sic07",
            "employees",
            "employment",
            "fte",
            "Name1",
            "Name2",
            "Name3",
            "Address1",
            "Address2",
            "Address3",
            "Address4",
            "Address5",
            "Postcode",
            "trading as 1",
            "trading as 2",
            "trading as 3",
            "region",
        ],
    )


def output_column_name_mapping():
    """
    Mapping to convert column names used in the pipeline to column names in the
    business template.
    """
    return {
        "period": "period",
        "classification": "SUT",
        "cell_no": "cell",
        "reference": "RU",
        "entname1": "name",
        "entref": "enterprise group",
        "frosic2007": "SIC",
        "formtype": "form type",
        "status": "status",
        "percentage_scotland": "%scottish",
        "percentage_wales": "%welsh",
        "froempment": "frozen employment",
        "sizeband": "band",
        "imputed_and_derived_flag": "imputed and derived flag",
        "statusencoded": "status encoded",
        "start date": "start date",
        "end date": "end date",
        "winsorised_value_exports": "returned to exports",
        "adjustedresponse_exports": "adjusted to exports",
        "imputed_and_derived_flag_exports": "imputed and derived flag exports",
        "statusencoded_exports": "status encoded exports",
        "winsorised_value_total_turnover": "returned turnover",
        "adjustedresponse_total_turnover": "adjusted turnover",
        "imputed_and_derived_flag_total_turnover": "imputed and derived flag turnover",
        "statusencoded_total_turnover": "status encoded turnover",
        "winsorised_value_water": "returned volume water",
        "adjustedresponse_water": "adjusted volume water",
        "imputed_and_derived_flag_water": "imputed and derived flag volume water",
        "statusencoded_water": "status encoded volume water",
    }


def devolved_outputs(
    df: pd.DataFrame,
    question_dictionary: dict,
    devolved_nation: str,
    local_unit_data: pd.DataFrame,
    devolved_questions: list = [11, 12, 40, 49, 110],  # 11, 12 might be 10, 11?
    agg_function: str = "sum",  # potential remove, here for testing
) -> pd.DataFrame:
    """
    Run to produce devolved outputs (excluding GB-NIR)
    Produced a pivot table and converts question numbers into plaintext
    for devolved questions
    """

    df = pd.merge(
        df,
        local_unit_data,
        left_on="reference",
        right_on="ruref",
        how="left",
        suffixes=["", "_local"],
    )
    df = filter_and_calculate_percent_devolved(df, local_unit_data, devolved_nation)

    devolved_dict = dict(
        (k, question_dictionary[k])
        for k in devolved_questions
        if k in question_dictionary
    )
    df["text_question"] = df["questioncode"].map(devolved_dict)

    pivot_index = [
        "period",
        "reference",
        "cell_no",
        "frosic2007",
        "formtype",
        "froempment",
    ]

    pivot_values = [
        "adjustedresponse",  # Original: adjusted_value -> adjustedresponse
        "winsorised_value",  # Imputated/winsorised (new_target_variable ->
        # adjustedresponse*outlier_weight)
        "imputed_and_derived_flag",  # [response_type -> imputed_and_derived_flag]
        "statusencoded",  # single letter (str) [error_mkr -> statusencoded]
    ]

    # Can use any agg function on numerical values, have to use lambda func for str cols
    pivot_agg_functions = [
        agg_function,
        agg_function,
        lambda x: "".join(str(v) for v in x if pd.notnull(v)),
        lambda x: "".join(str(int(v)) for v in x if pd.notnull(v)),
    ]

    dict_agg_funcs = dict(zip(pivot_values, pivot_agg_functions))

    df_pivot = pd.pivot_table(
        df,
        values=pivot_values,
        index=pivot_index,
        columns=["text_question"],
        aggfunc=dict_agg_funcs,
    )

    df_pivot.columns = ["_".join(col).strip() for col in df_pivot.columns.values]
    df_pivot = df_pivot[sorted(df_pivot.columns, key=split_func)]
    df_pivot.reset_index(inplace=True)

    # adding extra columns from df
    percent_devolved_nation_col = f"percentage_{devolved_nation.lower()}"

    extra_columns = [
        "period",
        "classification",
        "cell_no",
        "reference",
        "entname1",
        "entref",
        "frosic2007",
        "formtype",
        "status",
        percent_devolved_nation_col,
        "froempment",
        "sizeband",
    ]
    extra_columns_existing = [col for col in extra_columns if col in df.columns]
    df_extra = df[extra_columns_existing].drop_duplicates(
        subset=["period", "reference"]
    )

    merge_keys = [
        "period",
        "cell_no",
        "reference",
        "frosic2007",
        "formtype",
        "froempment",
    ]
    df_pivot = pd.merge(
        df_pivot,
        df_extra,
        on=merge_keys,
        how="left",
        suffixes=("", "_extra"),
    )

    # Drop the percentage column with the '_extra' suffix if it exists
    extra_col = f"{percent_devolved_nation_col}_extra"
    if extra_col in df_pivot.columns:
        df_pivot = df_pivot.drop(columns=[extra_col])

    # Now reorder columns as before, but only include columns that exist
    existing_desired_order = [col for col in extra_columns if col in df_pivot.columns]
    df_pivot = df_pivot[
        existing_desired_order
        + [col for col in df_pivot.columns if col not in existing_desired_order]
    ]

    # map the column names used in the pipeline to column names in the business template
    column_name_mapping = output_column_name_mapping()
    df_pivot = df_pivot.rename(columns=column_name_mapping)

    return df_pivot


def generate_devolved_outputs(additional_outputs_df=None, **config: dict) -> dict:
    """
    Main function to generate devolved outputs for nations specified in config.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing paths and settings.

    Returns
    -------
    dict
        Dictionary containing devolved outputs for each nation.

    Notes
    -----
    This function reads the input data, processes it to generate devolved outputs,
    and saves the results to CSV files. The function is designed to be flexible and
    can be easily adapted to handle different configurations and data formats.
    The devolved outputs are generated for the specified nations (e.g., Scotland,
    Wales) based on the provided configuration.

    Example usage:
    >>> config = {
    ...     "devolved_nations": ["Scotland", "Wales"],
    ...     "output_path": "path/to/data",
    ...     "winsorisation_file": "winsorisation_output.csv",
    ...     "lu_file": "ludets009_202303",
    ...     "lu_cols": ["ruref", "entref", "region"],
    ...     "question_no_plaintext": {
    ...         11: "start_date",
    ...         12: "end_date",
    ...         40: "total_turnover",
    ...         42: "commission_or_fees",
    ...         43: "sales_on_own_account",
    ...         46: "total_from_invoices",
    ...         47: "donations",
    ...         49: "exports",
    ...         110: "water",
    ...     },

    ...     "devolved_questions": [11, 12, 40, 49, 110],
    ... }
    >>> outputs = generate_devolved_outputs(config)
    for nation, df_pivot in outputs.items():
        print(f"================= Pivot table for {nation} =============")
        print(df_pivot)
    """

    logger.info(f"Generating devolved outputs for {config['devolved_nations']}")

    df = additional_outputs_df.copy()

    # local unit data
    lu_cols = get_lu_cols(config)
    lu_path = config.get("lu_path")
    lu_data = read_colon_separated_file(lu_path, lu_cols)

    question_no_plaintext = get_question_no_plaintext(config)
    devolved_questions = get_devolved_questions(config)

    nations = config.get("devolved_nations", ["Scotland", "Wales"])

    # Derive the band number from the cell_no column
    df["sizeband"] = np.where(
        df[config["cell_number"]].isna(),
        df[config["cell_number"]],
        df[config["cell_number"]].astype(str).str[-1],
    ).astype(int)

    outputs = {}
    for nation in nations:
        df_pivot = devolved_outputs(
            df,
            question_no_plaintext,
            devolved_nation=nation,
            local_unit_data=lu_data,
            devolved_questions=devolved_questions,
            agg_function="sum",
        )

        outputs[nation] = df_pivot
    return outputs
