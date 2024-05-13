import json
import pandas as pd
import os

data_folder = os.getcwd() + "/monthly-business-survey-results/playground/data/"

f = open(data_folder + 'snapshot-202212-002-2156d36b-e61f-42f1-a0f1-61d1f8568b8e.json')
 
data = json.load(f)

data.keys()

data["contributors"]
data["responses"]

contributors = pd.DataFrame(data["contributors"])
responses = pd.DataFrame(data["responses"])

contributors.T
responses