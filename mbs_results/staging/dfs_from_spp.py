from rdsa_utils.cdp.helpers.s3_utils import load_json
import json
import pandas as pd

def dfs_from_spp(
  client: str, 
  bucket_name: str, 
  filepath: str,
  import_platform: str,
) -> pd.DataFrame, pd.DataFrame:
  """
  Load in contributors and responses dataframes from SPP snapshot json, using either
  S3 buckets or data stored on network.

  Parameters
  ----------
  client : str
      Name of client for importing from S3 buckets
  bucket_name: str
      Name of bucket when importing from S3 buckets
  filepath : str
      Filepath of snapshot, either in S3 buckets or local filepath when importing from
      network
  import_platform : str
      Platform to import from. Must be either 's3' or 'network'

  Returns
  -------
  pd.DataFrame, pd.DataFrame
      Contributors and responses dataframes from snapshot.
  
  """
  
  if import_platform == "s3":
    snapshot = load_json(client, bucket_name, filepath)
  elif import_plaform == "network":
    with open(filepath, 'r') as f:
      snapshot = json.load(f)
  else: 
    raise Expection("platform must either be 's3' or 'network'")
    
  contributors = pd.DataFrame(snapshot['contributors'])
  responses = pd.DataFrame(snapshot['responses'])
  
  return contributors, responses