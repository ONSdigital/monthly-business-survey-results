import pandas as pd
from mbs_results.utilities.inputs import read_colon_separated_file


loaded_config = {
    "scottish_outputs": [
        "period",
        "SUT",
        "cell",
        "RU",
        "name",
        "enterprise group",
        "SIC",
        "form type",
        "status",
        "%scottish",
        "frozen employment",
        "band",
        "response type",
        "error marker",
        "start date",
        "end date",
        "returned turnover",
        "adjusted turnover",
        "response type.1",
        "error marker.1",
        "returned to exports",
        "adjused to exports",
        "response type.2",
        "error marker.2",
        "returned total employment",
        "adjusted total employment",
        "response type.3",
        "error marker.3",
        "returned FTM",
        "adjusted FTM",
        "response type.4",
        "error marker.4",
        "returned PTM",
        "adjusted PTM",
        "response type.5",
        "error marker.5",
        "returned FTF",
        "adjusted FTF",
        "response type.6",
        "error marker.6",
        "returned PTF",
        "adjusted PTF",
        "response type.7",
        "error marker.7",
        "returned volume water",
        "adjusted volume water",
        "response type.8",
        "error marker.8",
    ]
}

df = pd.read_csv("d:/consultancy/mbs_artifacts/temp_outputs_new_env/winsorisation_output_0.0.2.csv")
column_name = df.columns.to_list()
print(f"\nwinsorisation_output_column_name\n{column_name}")

print(f"\nscottish_outputs_column_name\n{loaded_config['scottish_outputs']}")

# Compute the difference between the two lists
difference_column_name = list(set(column_name) - set(loaded_config["scottish_outputs"]))
print("\nDifference between column_name and loaded_config['scottish_outputs']:")
print(difference_column_name)


# Compute the elements in loaded_config["scottish_outputs"] that are not in column_name
missing_columns = list(set(loaded_config["scottish_outputs"]) - set(column_name))

print("\nElements in loaded_config['scottish_outputs'] that are not in column_name:")
print(missing_columns)
print("=============================")


# Get finalsel dataframe
def get_finalsel():
    """
    Function to produce Scottish (and Welsh?) outputs
    Some data is not available from only MBS, do we need to request QBS data,
    is this for us to do or out of scope?

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    scotish_columns : list
        _description_

    Returns
    -------
    pd.DataFrame
        _description_
    """
    finalsel_filepath = "C:/Users/njobud/Office for National Statistics/Legacy Uplift - MBS/mbs_anonymised_jan_2025_20250128T111044/finalsel009_202112"
    
    finalsel_columns = [
        "ruref", "checkletter", "frosic2003", "rusic2003", "frosic2007", "rusic2007", 
        "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", 
        "frotover", "turnover", "entref", "wowentref", "vatref", "payeref", "crn", 
        "live_lu", "live_vat", "live_paye", "legalstatus", "entrepmkr", "region", 
        "birthdate", "entname1", "entname2", "entname3", "runame1", "runame2", 
        "runame3", "ruaddr1", "ruaddr2", "ruaddr3", "ruaddr4", "ruaddr5", 
        "rupostcode", "tradstyle1", "tradstyle2", "tradstyle3", "contact", 
        "telephone", "fax", "seltype", "inclexcl", "cell_no", "formtype", 
        "cso_tel", "currency"
    ]
    
    finalsel_data = read_colon_separated_file(finalsel_filepath, finalsel_columns)
       
    return finalsel_data


def scottish_outputs(df: pd.DataFrame, scotish_columns: list, sup_data: pd.DataFrame):
    """
    Function to produce Scottish (and Welsh?) outputs
    Some data is not available from only MBS, do we need to request QBS data,
    is this for us to do or out of scope?

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    scotish_columns : list
        _description_
    sup_data : pd.DataFrame
        _description_
    """
    return df[scotish_columns]

   
if __name__ == "__main__":
    df_scottish_outputs = scottish_outputs(df, loaded_config["scottish_outputs"], None)
    
    finalsel_data = get_finalsel()
    print(f"\n\nfinalsel_data\n{finalsel_data}")
