# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 10:54:27 2023
Create schema
@author: zoring

Read a CSV file, infer data types from data. Save the schema in TOML format.
"""
import os

import pandas as pd

# Input file location and name
data_dir = r"D:\data\mbs"
data_file = "outlier_output_v0.1.5_test_snaphot.csv"
schema_file = "outlier_output_schema.toml"

# Read the top 10 rows, inferrring the schema from csv
mypath = os.path.join(data_dir, data_file)

# Check the file exists
if not os.path.exists(mypath):
    raise FileNotFoundError(f"File not found: {mypath}")

df = pd.read_csv(mypath, nrows=10)

# Get column names  as data types as dict of strings
types = dict(df.dtypes)
schema = {col: str(types[col]) for col in types}

# Calculate the schema

# Initially, the schema is empty
S = ""

# Iterate through columns
for col in schema:
    s = f'[{col}]\nold_name = "{col}"\nDeduced_Data_Type = "{schema[col]}"\n\n'
    S = S + s

# Output the schema toml file
mypath = os.path.join(data_dir, schema_file)
text_file = open(mypath, "w")
text_file.write(S)
text_file.close()
