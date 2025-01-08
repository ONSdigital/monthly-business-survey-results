import pandas as pd

def map_column_values(df, column_name, mapping_dict, new_column_name):
    """
    Maps values in a specified column of a DataFrame based on a provided dictionary and creates a new column.
    If one value maps to multiple values, it creates extra rows.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column to be mapped.
    column_name (str): The name of the column to map values in.
    mapping_dict (dict): A dictionary where keys are original values and values are the new values.
    new_column_name (str): The name of the new column to store the mapped values.

    Returns:
    pd.DataFrame: The DataFrame with the new column containing the mapped values.
    """
    # Create a list to hold the new rows
    new_rows = []

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        original_value = row[column_name]
        if original_value in mapping_dict:
            mapped_values = mapping_dict[original_value]
            if isinstance(mapped_values, list):
                for value in mapped_values:
                    new_row = row.copy()
                    new_row[new_column_name] = value
                    new_rows.append(new_row)
            else:
                row[new_column_name] = mapped_values
                new_rows.append(row)
        else:
            new_rows.append(row)

    # Create a new DataFrame from the new rows
    new_df = pd.DataFrame(new_rows)
    return new_df

# Example usage:
df = pd.read_csv("C:/Users/daviel9/Office for National Statistics/Legacy Uplift - Testing outputs from DAP/MBS/mapping_files/form_domain_threshold_mapping.csv")
mapping_dict = {
    "T106G": "0106",
    "T111G": "0111",
    "T117G": "0117",
    "T167G": "0167",
    "T123G": "0123",
    "T173G": "0173",
    "MB01B": ["0201", "0202"],
    "MB03B": ["0203", "0204"],
    "MB15B": ["0205", "0216"],
    "T817G": "0817",
    "T823G": "0823",
    "T867G": "0867",
    "T873G": "0873"
}
mapped_df = map_column_values(df, 'form', mapping_dict, 'IDBR_form').reset_index(drop=True)
print(mapped_df)

mapped_df.to_csv("C:/Users/daviel9/Office for National Statistics/Legacy Uplift - Testing outputs from DAP/MBS/mapping_files/form_domain_threshold_mapping.csv", index=False)