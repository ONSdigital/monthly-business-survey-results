import os  # noqa F401

import pandas as pd
from src.utils.hdfs_mods import hdfs_load_json as read_json

from mbs_results.data_cleaning import (
    clean_and_merge,
    enforce_datatypes,
    load_manual_constructions,
)
from mbs_results.flag_and_count_matched_pairs import flag_matched_pair
from mbs_results.imputation_flags import create_impute_flags, generate_imputation_marker
from mbs_results.inputs import load_config
from mbs_results.utils import convert_column_to_datetime
from mbs_results.validation_checks import validate_config

# os.chdir("/home/cdsw/monthly-business-survey-results/")

config = load_config()
validate_config(config)
snapshot = read_json(config["mbs_results_path"])

df = clean_and_merge(snapshot=snapshot, **config)
df = enforce_datatypes(df, **config)
df = load_manual_constructions(df, **config)

df = pd.read_csv("tests/test_data_matched_pair/flag_pairs_2_groups_expected_output.csv")
df.rename(columns={"strata": "group", "target_variable": "return"}, inplace=True)
df = df.iloc[:, 0:4]
df.period = convert_column_to_datetime(df.period)
df["auxiliary"] = 10

# Simulate manual constructions
df["return_man"] = [100, None, None, None, 18, 27, None, None]

df = flag_matched_pair(df, forward_or_backward="f", **config)
df = flag_matched_pair(df, forward_or_backward="b", **config)
df = flag_matched_pair(
    df, forward_or_backward="f", **{**config, **{"target": "auxiliary"}}
)

df = create_impute_flags(df, **config)
df = generate_imputation_marker(df, config["target"])
