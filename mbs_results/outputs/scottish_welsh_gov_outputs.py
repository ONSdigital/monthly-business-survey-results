from pathlib import Path

import pandas as pd

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

def filter_and_calculate_percent_devolved(df,devolved_nation:str):
    devolved_nation = devolved_nation.lower()
    nation_to_code = {'scotland': 'XX', 'wales': 'WW'}
    try: 
        region_code = nation_to_code[devolved_nation]
    except:
        raise ValueError('devolved nation should be either Scotland or Wales')
    
    df = df.loc[df["region_local"] == region_code]
    df['percentage_'+devolved_nation] = -1
    # Continue with calculation for percentage ...

    return df
    


def devolved_outputs(
    df: pd.DataFrame,
    question_dictionary: dict,
    devolved_nation: str,
    local_unit_data: pd.DataFrame,
    devolved_questions: list = [11, 12, 40, 49, 110], # 11, 12 might be 10, 11?
    agg_function: str = "sum", # potential remove, here for testing
) -> pd.DataFrame:
    """
    Run to produce devolved outputs (ex GB-NIR)
    Produced a pivot table and converts question numbers into
    """

    df = pd.merge(df, local_unit_data, left_on='reference', right_on='ruref',how='left',suffixes = ['', '_local'])
    df = filter_and_calculate_percent_devolved(df,devolved_nation)

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
        values=["adjusted_value", "new_target_variable", "response_type", "error_mkr"],
        index=pivot_index,
        columns=["text_question"],
        aggfunc=dict_agg_funcs,
    )

    df_pivot.columns = ["_".join(col).strip() for col in df_pivot.columns.values]
    df_pivot = df_pivot[sorted(df_pivot.columns, key=split_func)]

    return df_pivot


if __name__ == "__main__":

    data_path = Path(r"D:\temp_outputs_new_env")
    df = pd.read_csv(data_path / "winsorisation_output_0.0.2.csv")
    # df = df.sample(n = 10000)

    question_no_plaintext = {
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

    devolved_questions = [40, 110, 49, 11, 12]

    from mbs_results.utils import read_colon_separated_file

    lu_cols = [
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
    ]

    lu_data = read_colon_separated_file(
        data_path / "ludets009_202303" / "ludets009_202303", lu_cols
    )
    df_pivot = devolved_outputs(df, question_no_plaintext,devolved_nation='Wales',local_unit_data = lu_data)
    print(df_pivot)
    df_pivot = devolved_outputs(df, question_no_plaintext,devolved_nation='Scotland',local_unit_data = lu_data)
    print(df_pivot)

    # region_local: wales - 25174, scotland - 42020
    # region (finalsel): wales - 16173, scotland - 30397