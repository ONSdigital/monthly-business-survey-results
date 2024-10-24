from mbs_results.staging.dfs_from_spp import dfs_from_spp
from mbs_results.utilities.inputs import load_config

config = load_config()
contributors, responses = dfs_from_spp(
    config["folder_path"] + config["mbs_file_name"],
    config["platform"],
    config["bucket"],
)
