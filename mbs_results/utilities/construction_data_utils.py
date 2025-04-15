import json
import pandas as pd

def get_construction_dataframes(construction_snapshot):
    """
    Function to convert Construction JSON snapshot into
    Responses and Contributors DataFrames
    """

    with open(construction_snapshot, mode="r") as f:
        snapshot_json = json.load(f)

    contributors_df = pd.DataFrame(data=snapshot_json["contributors"])
    responses_df = pd.DataFrame(data=snapshot_json["responses"])


    # Type conversions

    # Pandas automatic type conversion fails here when converting numeric types,
    # so I'm explicitly seperating the columns to apply pd.to_numeric() to only 
    # the numeric columns

    # Todo: these lists should be given through a config, not defined here

    contributors_non_numeric = [
        "status",
        "receiptdate",
        "lockedby",
        "lockeddate",
        "formtype",
        "checkletter",
        "currency",
        "payereference",
        "companyregistrationnumber",
        "reportingunitmarker",
        "region",
        "birthdate",
        "referencename",
        "referencepostcode",
        "tradingstyle",
        "selectiontype",
        "inclusionexclusion",
        "createdby",
        "createddate",
        "lastupdatedby",
        "lastupdateddate",
    ]

    responses_non_numeric = [
        "createdby",
        "createddate",
        "lastupdatedby",
        "lastupdated_date",
    ]

    contributors_clean_df = type_convert_by_cols(
        contributors_df, contributors_non_numeric
    )

    responses_clean_df = type_convert_by_cols(
        responses_df, responses_non_numeric
    )


    # Return type converted DataFrames

    return contributors_clean_df, responses_clean_df


def type_convert_by_cols(df, non_numeric_cols):
    """
    Small helper function to aid in type conversion
    """

    numeric_cols = [
        item for item in df.columns if item not in non_numeric_cols
    ]

    for col in numeric_cols:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception as e:
            print(f"There was a problem converting {col}:", e)

    # Apply automatic conversion to clean up the non-numeric data
    return df.convert_dtypes()
