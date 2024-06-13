import os  # noqa F401

import pandas as pd
from src.utils.hdfs_mods import hdfs_load_json as read_json

from mbs_results.inputs import load_config

# os.chdir("/home/cdsw/monthly-business-survey-results/")


# TODO:
# run modules
# run imputation
# deal with QA outputs
# basic logging
# kwargs in config

config = load_config()
snapshot = read_json(config["mbs_results_path"])

contributors = pd.DataFrame(snapshot["contributors"])
responses = pd.DataFrame(snapshot["responses"])
