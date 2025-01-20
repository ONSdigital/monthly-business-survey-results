from ast import IsNot
import pandas as pd


def constrains_validation(
    df: pd.DataFrame,
    target_column: str,
    marker_column: str):
    
    incorrect_records = []

    for index, row in df.iterrows():
        if (row[target_column] > 40) & (pd.notna(row[marker_column])):
            incorrect_records.append(row)

    print(f'Number of missed constrain records: {len(incorrect_records)}')

    return incorrect_records


