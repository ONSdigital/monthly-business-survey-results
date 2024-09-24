from pathlib import Path

import pandas as pd

# Missing reporting unit RU and name
# Should SIC be frozenSIC or SIC)5_digit
# Missing RU, name, enterprise group, status? start and end date questions removed?
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


def devolved_outputs(
    df: pd.DataFrame,
    question_dictionary: dict,
    devolved_questions: list = [40, 110, 49, 11, 12],
    agg_function: str = "sum",
    local_unit_data: pd.DataFrame = 0,
) -> pd.DataFrame:
    """
    Run to produce devolved outputs (ex GB-NIR)
    Produced a pivot table and converts question numbers into
    """

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
        "sic_5_digit",
        "form_type",
        "froempees",
    ]

    pivot_values = [
        "adjusted_value",
        "new_target_variable",
        "response_type",
        "error_mkr",
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
    print(df.shape)
    df = df.sample(n=10000, random_state=1)

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
    df_pivot = devolved_outputs(df, question_no_plaintext)
    print(df_pivot)
