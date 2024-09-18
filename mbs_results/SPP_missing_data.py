# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 11:55:09 2024

@author: matthk
"""

import pandas as pd
import numpy as np
import json
import os

os.chdir('T:\Statistical Team\Katey\MBS\SPP Missing Data')

#Import the json file
#snapshot = pd.read_json('snapshot_009_202303_1.json')

with open('snapshot_009_202303_1.json') as f :
    snapshot = json.load(f)
    
contributors_df = pd.json_normalize(snapshot["contributors"])
responders_df = pd.json_normalize(snapshot["responses"])

#filter and add missing rows
#filter on form ID
form_id ="009"
responders_filtered = responders_df[responders_df['survey'] == form_id]
#define required questioncode values
questioncode_values = [40,49]

#set up list to collect missing rows needed
missing_rows = []

# Iterate over each reference and period combination in contributors
for _, row in contributors_df.iterrows():
    ref, period = row['reference'], row['period']
    
    # Filter responders for reference and period
    nonmissing_rows = responders_filtered[(responders_filtered['reference'] == ref) & (responders_filtered['period'] == period)]
    
    existing_qcodes = nonmissing_rows['questioncode'].tolist()
    
    # Add only the missing questioncode values
    for code in questioncode_values:
        if code not in existing_qcodes:
            missing_rows.append({
                'reference': ref, 
                'period': period, 
                'formID': form_id, 
                'questioncode': code, 
                'value_1': np.nan, 
                'value_2': np.nan
            })

# Convert missing_rows to DataFrame
missing_rows_df = pd.DataFrame(missing_rows)

# Append missing rows to the original responders df
responders_all = pd.concat([responders_df, missing_rows_df], ignore_index=True)

# Sort df
responders_all = responders_all.sort_values(by=['reference', 'period', 'questioncode']).reset_index(drop=True)

###check specific rus
ru_filtered = responders_df[responders_df['reference'] == '11000065602']
ru_filtered_2 = responders_df[responders_df['reference'] == '11543783815']
ru_filtered_3 = responders_all[responders_all['reference'] == '11543783815']




####################################
################################
### Corrected response adjusted response cols

# Filter and add missing rows
# Filter on form ID
form_id = "009"
responders_filtered = responders_df[responders_df['survey'] == form_id]

# Define required questioncode values
questioncode_values = [40, 49]

# Set up list to collect missing rows needed
missing_rows = []

# Iterate over each reference and period combination in contributors
for _, row in contributors_df.iterrows():
    ref, period = row['reference'], row['period']
    
    # Filter responders for reference and period
    nonmissing_rows = responders_filtered[(responders_filtered['reference'] == ref) & (responders_filtered['period'] == period)]
    
    existing_qcodes = nonmissing_rows['questioncode'].tolist()
    
    # Add only the missing questioncode values
    for code in questioncode_values:
        if code not in existing_qcodes:
            if not nonmissing_rows.empty:
                # Get the base row to copy other columns
                base_row = nonmissing_rows.iloc[0].to_dict()
            else:
                # If no rows exist for a reference and period, create an empty base row
                base_row = {
                    'instance': np.nan,
                    'createdby': np.nan,
                    'createddate': np.nan,
                    'lastupdatedby': np.nan,
                    'lastupdateddate': np.nan
                }

            # Create the missing row with appropriate values
            missing_row = {
                'reference': ref, 
                'period': period, 
                'survey': form_id, 
                'questioncode': code, 
                'response': np.nan, 
                'adjustedresponse': np.nan,
                'instance': base_row['instance'], 
                'createdby': base_row['createdby'], 
                'createddate': base_row['createddate'], 
                'lastupdatedby': base_row['lastupdatedby'], 
                'lastupdateddate': base_row['lastupdateddate']
            }
            
            missing_rows.append(missing_row)

# Convert missing_rows to DataFrame
missing_rows_df = pd.DataFrame(missing_rows)

# Append missing rows to the original responders df
responders_all = pd.concat([responders_df, missing_rows_df], ignore_index=True)

# Sort df
responders_all = responders_all.sort_values(by=['reference', 'period', 'questioncode']).reset_index(drop=True)
