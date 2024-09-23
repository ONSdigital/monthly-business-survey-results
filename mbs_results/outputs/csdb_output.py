import pandas as pd 
from pathlib import Path
data_path = Path(r"D:\temp_outputs_new_env")
metadata_path = Path(r"D:\tmp")

df = pd.read_csv(data_path / 'winsorisation_output_0.0.2.csv')
print(df.columns)

cdid_schema = pd.read_csv(metadata_path/"csdb_raw_data_metadata.csv")
cdid_schema.rename(columns={"SIC on CS":"classification", "Question Number on CS":"question_no"},inplace=True)
cdid_schema = cdid_schema[["cdid","question_no","classification"]]
print(cdid_schema)
# All SIC on CS was found to match exactly with the classification

df_join = pd.merge(left= df,right= cdid_schema, on=["question_no","classification"],how="left")
print(df_join.loc[(df_join["classification"].notna()) & (df_join["cdid"].isna())][["question_no","classification","cdid"]])

class_list = cdid_schema["classification"].to_list()
seen = set()
dupes = [x for x in class_list if x in seen or seen.add(x)]  
print(dupes)# ? Should it be outer join and allow for different classifications to be in different cdid?

question_no_plaintext = {"40":"gross "}