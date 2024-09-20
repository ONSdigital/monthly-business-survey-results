import pandas as pd
from pathlib import Path

data_path = Path(r"D:\temp_outputs_new_env")
df = pd.read_csv(data_path / 'winsorisation_output_0.0.2.csv')
print(df.shape)
df = df.sample(n=10000, random_state=1)

# Missing reporting unit RU and name
pivot_index = ["period","reference","cell_no","sic_5_digit", "form_type", "froempees"]

question_no_plaintext = {
    11 : "start_date",
    12 : "end_date",
    40 : "total_turnover",
    42 : "commission_or_fees",
    43 : "sales_on_own_account",
    46 : "total_from_invoices",
    47 : "donations",
    49 : "exports",
    110 : "water"
    }

scot_outputs = [40, 110, 49, 11, 12]
scot_dict = dict((k, question_no_plaintext[k]) for k in scot_outputs if k in question_no_plaintext)

df["text_question"] = df["question_no"].map(scot_dict)

df2 = pd.pivot_table(df, values=["adjusted_value",'new_target_variable',"type","response_type"], index=pivot_index,
                       columns=["text_question"], aggfunc="sum")
print(df2.columns)
df2.columns = ['_'.join(col).strip() for col in df2.columns.values]
print(df2.columns)
print(df2)
def split_func(my_string:str):
    return my_string.split(sep = ('_'))[-1]
df2 = df2[sorted(df2.columns,key = split_func)]
