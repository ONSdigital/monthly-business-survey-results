from mbs_results.staging.dfs_from_spp import dfs_from_spp
from mbs_results.inputs import load_config

config = load_config()
contributors, responses = dfs_from_spp(
  config["client"], config["bucket_name"], config["filepath"], config["platform"]
)
