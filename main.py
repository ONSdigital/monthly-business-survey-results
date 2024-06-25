import os  # noqa F401

from src.utils.hdfs_mods import hdfs_load_json as read_json

from mbs_results.data_cleaning import clean_and_merge, enforce_datatypes
from mbs_results.inputs import load_config

# os.chdir("/home/cdsw/monthly-business-survey-results/")

config = load_config()
snapshot = read_json(config["mbs_results_path"])

df = clean_and_merge(snapshot=snapshot, **config)
df = df.reset_index()
df = enforce_datatypes(df, **config)
print(df.dtypes)
