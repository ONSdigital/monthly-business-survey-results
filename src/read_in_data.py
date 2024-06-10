import json

import pandas as pd
import pydoop.hdfs as hdfs

folder_path = "/dapsen/workspace_zone/mbs-results/"
file_name = "snapshot-202212-002-2156d36b-e61f-42f1-a0f1-61d1f8568b8e.json"

file_path = folder_path + file_name

with hdfs.open(file_path, "r") as f:
    data = json.load(f)

contributors = pd.DataFrame(data["contributors"])
responses = pd.DataFrame(data["responses"])
