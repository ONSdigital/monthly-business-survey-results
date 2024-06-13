import pandas as pd

from src.utils.hdfs_mods import hdfs_load_json as read_json

# TODO: read from config
folder_path = "/dapsen/workspace_zone/mbs-results/"
file_name = "snapshot-202212-002-2156d36b-e61f-42f1-a0f1-61d1f8568b8e.json"
file_path = folder_path + file_name

snapshot = read_json(file_path)

contributors = pd.DataFrame(snapshot["contributors"])
responses = pd.DataFrame(snapshot["responses"])
