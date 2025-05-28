from pathlib import Path
import pandas as pd
from mbs_results.utilities.inputs import read_colon_separated_file
from mbs_results import logger

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
    df: pd.DataFrame, devolved_nation: str
) -> pd.DataFrame:
    """Filter by devolved nation and calculate percentage column"""
    devolved_nation = devolved_nation.lower()
    nation_to_code = {'scotland': 'XX', 'wales': 'WW'}
    if devolved_nation not in nation_to_code:
        error_msg = (
            f"Invalid devolved nation '{devolved_nation}'. "
            "Expected 'Scotland' or 'Wales'."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    region_code = nation_to_code[devolved_nation]
    
    logger.info(f"Filtering for {devolved_nation} with region code {region_code}")
    logger.info(f"Columns before filtering: {df.columns}")
    #df = df.loc[df["region_local"] == region_code].copy()
    #logger.info(f"Columns after filtering: {df.columns}")
    percent_col = f'percentage_{devolved_nation}'
    df['percentage_' + devolved_nation] = -1
    # Continue with calculation for percentage ...
    print(f"Calculating percentage for {devolved_nation}")
    df[percent_col] = (df['adjustedresponse'] / df['new_target_variable']) * 100
    return df


def get_devolved_questions() -> list:
    """Get devolved questions from config or use default."""
    return [11, 12, 40, 49, 110]


def get_question_no_plaintext(config: dict) -> dict:
    """Get question number to plaintext mapping from config or use default."""

    return {
        11: "start_date",
        12: "end_date",
        40: "total_turnover",
        42: "commission_or_fees",
        43: "sales_on_own_account",
        46: "total_from_invoices"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ,
        47: "donations",
        49: "exports",
        110: "water",
    }


def get_lu_cols(config: dict) -> list:
    """Get local unit columns from config or use default."""
    return [
        "ruref", "entref", "lu ref", "check letter", "sic03", "sic07", "employees",
        "employment", "fte", "Name1", "Name2", "Name3", "Address1", "Address2",
        "Address3", "Address4", "Address5", "Postcode", "trading as 1", "trading as 2",
        "trading as 3", "region"
    ]


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
        left_on='reference',
        right_on='ruref',
        how='left',
        suffixes=['', '_local']
    )
    df = filter_and_calculate_percent_devolved(df, devolved_nation)

    devolved_dict = dict(
        (k, question_dictionary[k])
        for k in devolved_questions
        if k in question_dictionary
    )
    df["text_question"] = df["question_no"].map(devolved_dict)

    pivot_index = [
        "period",
        "reference",
        "cell_no",
        "frosic2007",
        "form_type",
        "froempees",
    ]

    pivot_values = [
        "adjusted_value", # Original 
        "new_target_variable", # Imputated / winsorised
        "response_type", # numeric response type
        "error_mkr", # single letter (str)
    ]

    # Can use any agg function on numerical values, have to use lambda func for str cols
    pivot_agg_functions = [
        agg_function,
        agg_function,
        agg_function,
        lambda x: " ".join(x),
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
    
    Raises
    ------
    ValueError
        If the devolved nation is not recognised.
        
    KeyError
        If the specified columns are not found in the DataFrame.
        
    TypeError
        If the input DataFrame is not a pandas DataFrame.
        
    
    
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
    ...     "data_path": "path/to/data",
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

    # The paths could be added to the config
    data_path = Path(r"D:/consultancy/mbs_artifacts/temp_outputs_new_env")
    winsorisation_file = data_path / "winsorisation_output_0.0.2.csv"
    lu_file = data_path / "ludets009_202303" / "ludets009_202303"

    if additional_outputs_df is not None:
        df = additional_outputs_df.copy()
        print(f"\nUsing additional outputs DataFrame with shape: {df.shape}")
        print(f"Columns in additional outputs DataFrame: {df.columns}")
        print(f"df[['period', 'adjustedresponse', 'response']]:\n{df[['period', 'adjustedresponse', 'response']].head(10)}")
        
        df_winzor = pd.read_csv(winsorisation_file)
        print(f"\nUsing winsorisation DataFrame with shape: {df_winzor.shape}")
        print(f"Columns in winsorisation DataFrame: {df_winzor.columns}")
        print(f"df_winzor[['period', 'adjusted_value']]:\n{df_winzor[['period', 'adjusted_value', 'new_target_variable']].head(10)}")
    else:
        df = pd.read_csv(winsorisation_file)
    lu_cols = get_lu_cols(config)
    lu_data = read_colon_separated_file(lu_file, lu_cols)

    question_no_plaintext = get_question_no_plaintext(config)
    devolved_questions = get_devolved_questions()
    nations = config.get("devolved_nations", ["Scotland", "Wales"])

    outputs = {}
    for nation in nations:
        df_pivot = devolved_outputs(
            df,
            question_no_plaintext,
            devolved_nation=nation,
            local_unit_data=lu_data,
            devolved_questions=devolved_questions,
            agg_function="sum"
        )
        
        # TO DO: 
        #     1: rename the columns to match the business required column names
        #           df_pivot.rename(columns=rename_dict, inplace=True)
        #     2: Rearrange the columns to match the business required column order: column_order
        #           df_pivot = df_pivot[column_order]
        #     3: Add the RU and name columns to the pivot table
        #           df_pivot = pd.merge(df_pivot, lu_data, left_on='reference', right_on='ruref', how='left')
        
        output_file = data_path / f"{nation.lower()}_pivot.csv"
        df_pivot.to_csv(
            output_file,
            index=False,
            sep=",",
            quoting=1,
        )
        outputs[nation] = df_pivot
    return outputs


if __name__ == "__main__":
    # region_local: wales - 25174, scotland - 42020
    # region (finalsel): wales - 16173, scotland - 30397
    
    column_names = {
        "period": "period",
        "sut": "SUT",
        "cell_no": "cell",
        "reference": "RU",
        "entname1": "name",
        "enterprice_group": "enterprice group",
        "frosic2007": "SIC",
        "form_type": "form type",  
    }

    outputs = generate_devolved_outputs(additional_outputs_df=None, config={},)
    for nation, df_pivot in outputs.items():
        print(f"================= Pivot table for {nation} =============")
        print(df_pivot)
