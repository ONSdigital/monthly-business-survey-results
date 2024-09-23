from pathlib import Path

import pandas as pd

# Missing reporting unit RU and name
# Should SIC be frozenSIC or SIC)5_digit


def split_func(my_string: str):
    return my_string.split(sep=("_"))[-1]


# Missing RU, name, enterprise group, status? start and end date questions removed?


def devolved_outputs(
    df: pd.DataFrame,
    question_dictionary: dict,
    devolved_questions: list = [40, 110, 49, 11, 12],
) -> pd.DataFrame:
    # Temp sample to reduce comp
    df = df.sample(n=10000, random_state=1)

    devolved_dict = dict(
        (k, question_no_plaintext[k])
        for k in devolved_questions
        if k in question_no_plaintext
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

    df_pivot_numeric = pd.pivot_table(
        df,
        values=["adjusted_value", "new_target_variable", "response_type", "error_mkr"],
        index=pivot_index,
        columns=["text_question"],
        aggfunc="sum",
    )

    df_pivot_char = pd.pivot_table(
        df,
        values=["adjusted_value", "new_target_variable", "response_type", "error_mkr"],
        index=pivot_index,
        columns=["text_question"],
        aggfunc=lambda x: " ".join(x),
    )
    print(df_pivot_numeric.index.names)
    df_pivot = pd.merge(
        df_pivot_numeric, df_pivot_char, on=df_pivot_numeric.index.names
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
    df_pivot.to_csv("pivot.csv")
