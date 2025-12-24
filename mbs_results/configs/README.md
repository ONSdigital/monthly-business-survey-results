# User Config

| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| bucket | The path to the bucket. | string | Any filepath. |
| ssl_file | The path to the ssl certificate. | string | Any filepath. |
| calibration_group_map_path | The filepath to the calibration group mapping file. | string | Any filepath. |
| classification_values_path | The filepath to the file containing SIC classification values. | string | Any filepath. |
| snapshot_file_path | The full filepath to the snapshot data | string | Any filepath. |
| idbr_folder_path | The path to the folder containing the IDBR data. | string | Any filepath. |
| l_values_path | The filepath to the file containing l values. | string | Any filepath. |
| manual_constructions_path | The filepath to the file containing manual constructions data. | string | Any filepath. |
| filter | The filepath to the data to filter out from the imputation method (optional). | string | Any filepath. |
| manual_outlier_path | The filepath to the data containing manual outliers (optional) | string | Any filepath. |
| output_path | The filepath where outputs should be saved to. | string | Any filepath. |
| population_prefix | The prefix for the population files. | string | Any string. |
| sample_prefix | The prefix for the sample filenames. | string | Any string. |
| population_counts_prefix | The base name to use for population counts files. | string | Any string. |
| back_data_qv_path | The filepath for the file containing QV backdata. | string | Any filepath. |
| back_data_cp_path | The filepath for the file containing CP backdata. | string | Any filepath. |
| back_data_qv_cp_json_path | The filepath for the JSON file containing back_data. | string | Any filepath. |
| back_data_finalsel_path | The filepath for the file containing final selection backdata. | string | Any filepath. |
| sic_domain_mapping_path | The filepath for the data containing the mapping from SIC codes to domains. | string | Any filepath. |
| threshold_filepath | The filepath for the data containing thresholds for selective editing. | string | Any filepath. |
| current_period | The most recent period to include in the outputs (same as above). | int | Any int in the form `yyyymm`. |
| revision_window | The number of months to use as a revision window. | int | Any int in the form `mm` or `m` (does not need to be zero-padded). |
| state | Whether to run the pipeline in a frozen or live state. | string | `"frozen"` or `"live"`.|
| devolved_nations | The nations to include in the devolved nations output. | list of strings. | A list containing either `"Scotland"` and/or `"Wales"`. |
| optional_outputs | A list of optional outputs to produce after the pipeline has run. | list | Any of the outputs listed in `mbs_results/outputs/produce_additional_outputs.py` within the `produce_additional_outputs` function which can be produced. |
| generate_schemas | Setting to control if schemas are automatically generated for outputs | bool | `True` or `False` |
| schema_path | Location to save generated schemas | string | Any filepath |
| state | To run the pipeline with `frozen` or `live` status. | string | Either `frozen` or `live`. |
| debug_mode | Whether to export all the intermediate methods outputs (imputation, estimation, winsorisation) . | bool | Either `true` or `false`. |
| run_id | Identifier to tag outputs and logs for a specific run. | string | Any string (e.g. timestamp `YYYYMMDDHHMM`). |
| split_methods_outputs_by_period | Whether to split the methods outputs into separate outputs based on the period. | bool | Either `true` or `false` |
| split_qa_output_by_period | Whether to split the qa output into separate outputs based on the period. | bool | Either `true` or `false` |
| split_turnover_output_by_period | Whether to split the turnover output into separate outputs based on the period. | bool | Either `true` or `false` |
| split_results_output_by_period | Whether to split the results output into separate outputs based on the period. | bool | Either `true` or `false` |

# Outputs Config

| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| bucket | The path to the bucket. | string | Any filepath. |
| ssl_file | The path to the ssl certificate. | string | Any filepath. |
| idbr_folder_path | The path to the folder containing the IDBR data. | string | Any filepath. |
| snapshot_file_path | The full filepath to the snapshot data | string | Any filepath. |
| main_mbs_output_folder_path | The folder path containing the methods outputs to read from. | string | Any filepath. |
| mbs_output_prefix | The base filename prefix for the main MBS methods output. | string | Any filename base. |
| population_counts_prefix | The base filename prefix for the population counts output. | string | Any filename base. |
| ludets_prefix | The base filename prefix for the ludets file. | string | Any filename base. |
| output_path | The filepath where outputs should be saved to. | string | Any filepath. |
| cdid_data_path | The filepath to the file containing cdid data. | string | Any filepath. |
| current_period | The most recent period to include in the outputs (same as above). | int | Any int in the form `yyyymm`. |
| revision_window | The number of months to use as a revision window. | int | Any int in the form `mm` or `m` (does not need to be zero-padded). |
| devolved_nations | Nations to create outputs for choose between `Scotland`, `Wales`. | List of string | List of nations. |
| run_id | Identifier to tag outputs and logs for a specific run. | string | Any string (e.g. timestamp `YYYYMMDDHHMM`). |
| split_output_by_period | Any additional output named in this list will be split into multiple outputs based on period | List of string | List of additional outputs. Currently will only work with `"produce_ocea_srs_outputs"` (config_user.json is used for the turnover output and the qa output).|

# Export Config

| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| platform | Specifies whether you're running the pipeline locally or on DAP. | string | `"network"`, `"s3"` |
| bucket | The path to the bucket. | string | Any filepath. |
| ssl_file | The path to the ssl certificate. | string | Any filepath. |
| output_dir | The path to the folder containing the files to be exported from. | string | Any filepath. |
| export_dir | The path to the folder containing the files to be exported to. | string | Any filepath. |
| schemas_dir | The path to the folder containing the schema toml data, if empty the export headers in manifest will be set to empty string. | string | Any filepath. |
| run_id | Identifier appended to exported filenames (before versioning) and used in manifest. | string | Any string (e.g. timestamp `YYYYMMDDHHMM`). |
| copy_or_move_files | Whether to copy or move the listed files. | string | `"copy"`, `"move"` |
| files_to_export | Toggle flags for which files to export. | dictionary | Any dictionary in the format `{"output_name": true/false}` |
| files_basename | The base name for a file. | dictionary of strings | Any dictionary in the format `{"file_basename": "output_name"}` |
e.g the example below has run_id `202511071451` , methods_output set to `true` and methods_output basename `mbs_results`, thus will export only the file `mbs_results_202511071451.csv` and create the relevant manifest file:
```
"run_id": "202511071451",

"files_to_export": {
    "methods_output": true,
    "growth_rates_output": false,
    "create_csdb_output": false,
    "create_imputation_link_output": false,
    "population_counts": false,
    "produce_ocea_srs_outputs": false,
    "produce_qa_output": false,
    "scotland_generate_devolved_outputs": false,
    "turnover_output": false,
    "wales_generate_devolved_outputs": false
},
"files_basename": {
    "methods_output": "mbs_results",
    "growth_rates_output": "growth_rates_output",
    "create_csdb_output": "create_csdb_output",
    "create_imputation_link_output": "create_imputation_link_output",
    "population_counts": "mbs_population_counts",
    "produce_ocea_srs_outputs": "produce_ocea_srs_outputs",
    "produce_qa_output": "produce_qa_output",
    "scotland_generate_devolved_outputs": "scotland_generate_devolved_outputs",
    "turnover_output": "turnover_output",
    "wales_generate_devolved_outputs": "wales_generate_devolved_outputs"
    }
```

You may extend the `files_to_export` and `files_basename` dictionaries with more outputs if required.

## Guidance for additional outputs
As an end user, you will only need to change the export config (named `config_export.json`). The process will copy (or move) the files listed in the `files` section from the defined `output_dir` to `export_dir`, and will create a manifest json file for the NiFi.

# Dev Config
| Parameter | Description | Default | Data Type | Acceptable Values |
|---|---|---|---|---|
| platform | Specifies whether you're running the pipeline locally or on DAP. | `"s3"` | string | `"network"`, `"s3"` |
| back_data_type | The name of the backdata type marker column. | `"type"` | string | Any valid column name. |
| back_data_format | The file type to use for back data | `"json"` | string | `"csv"`, `"json"` |
| imputation_marker_col | The name of the column being used as an imputation marker. | `"imputation_flags_adjustedresponse"` | string | Any valid column name. |
| auxiliary | The name of the column containing the auxiliary variable. | `"frotover"` | string | Any valid column name. |
| auxiliary_converted | The name of the column containing the auxiliary variable converted into monthly actual pounds. | `"converted_frotover"` | string | Any valid column name. |
| calibration_factor | The name of the column containing the calibration factor variable. | `"calibration_factor"` | string | Any valid column name. |
| cell_number | The name of the column containing the cell number variable. | `"cell_no"` | string | Any valid column name. |
| design_weight | The name of the column containing the design weight variable. | `"design_weight"` | string | Any valid column name. |
| status | The name of the column containing the status variable. | `"statusencoded"` | string | Any valid column name. |
| form_id_idbr | The name of the column containing the form type (IDBR) variable. | `"formtype"` | string | Any valid column name. |
| sic | The name of the column containing the SIC variable. | `"frosic2007"` | string | Any valid column name. |
| group | The name of the column containing the group variable. | `"calibration_group"` | string | Any valid column name. |
| calibration_group | The name of the column containing the calibration group variable. | `"calibration_group"` | string | Any valid column name. |
| period | The name of the column containing the period variable. | `"period"` | string | Any valid column name. |
| question_no | The name of the column containing the question number/code variable. | `"questioncode"` | string | Any valid column name. |
| reference | The name of the column containing the reference variable. | `"reference"` | string | Any valid column name. |
| region | The name of the column containing the region variable. | `"region"` | string | Any valid column name. |
| sampled | The name of the column containing the is_sampled variable. | `"is_sampled"` | string | Any valid column name. |
| census | The name of the column containing the is_census variable. | `"is_census"` | string | Any valid column name. |
| strata | The name of the column containing the strata variable. | `"cell_no"` | string | Any valid column name. |
| target | The name of the column containing the target variable. | `"adjustedresponse"` | string | Any valid column name. |
| form_id_spp | The name of the column containing the form type (SPP) variable. | `"form_type_spp"` | string | Any valid column name. |
| l_value_question_no | The name of the column holding the question number in the l-values dataset. | `"question_no"` | string | Any valid column name. |
| nil_status_col | The name of the column indicating NIL statuses. | `"status"` | string | Any valid column name. |
| pound_thousand_col | The name of the column containing the target in pounds-thousands. | `"adjustedresponse_pounds_thousands"` | string | Any valid column name. |
| master_column_type_dict | Defines the expected data types for various columns. | `{"reference": "int", "period": "date", "response": "str", "questioncode": "int", "adjustedresponse": "float", "frozensic": "str", "frozenemployees": "int", "frozenturnover": "float", "cellnumber": "int", "formtype": "str", "status": "str", "statusencoded": "int", "frosic2007": "str", "froempment": "int", "frotover": "float", "cell_no": "int"}` | dict | Any dictionary in the format `{"column_name": "data_type"}` where column name is a valid column and data_type is one of `"bool"`, `"int"`, `"str"` or `"float"`. Both key and value should be enclosed in quotation marks. |
| contributors_keep_cols | Columns to keep for contributors. | `["period", "reference", "status", "statusencoded"]` | list | A list of valid column names. |
| responses_keep_cols | Columns to keep for responses. | `["adjustedresponse", "period", "questioncode", "reference", "response"]` | list | A list of valid column names. |
| finalsel_keep_cols | Columns to keep for final selection. | `["formtype", "cell_no", "froempment", "frotover", "reference", "entname1", "runame1"]` | list | A list of valid column names. |
| temporarily_remove_cols | Columns to temporarily remove. | [] | list | A list of valid column names. |
| non_sampled_strata | Non-sampled strata values. | `["5141", "5142", "5143", "5371", "5372", "5373", "5661", "5662", "5663"]` | list | A list of cell numbers/strata. |
| population_column_names | Column names in the population frame. | `["reference", "checkletter", "inqcode", "entref", "wowentref", "frosic2003", "rusic2003", "frosic2007", "rusic2007", "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", "frotover", "turnover", "entrepmkr", "legalstatus", "inqstop", "entzonemkr", "region", "live_lu", "live_vat", "live_paye", "immfoc", "ultfoc", "cell_no", "selmkr", "inclexcl" ]` | list | A list of valid column names in the population frame dataset. |
| population_keep_columns | Population columns to keep. | `["reference", "region", "frotover", "cell_no"]` | list | A list of valid column names in the population frame dataset. |
| sample_column_names | Column names in the sample data. | `["reference", "checkletter", "frosic2003", "rusic2003", "frosic2007", "rusic2007", "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", "frotover", "turnover", "entref", "wowentref", "vatref", "payeref", "crn", "live_lu", "live_vat", "live_paye","legalstatus", "entrepmkr", "region", "birthdate", "entname1", "entname2", "entname3", "runame1", "runame2","runame3", "ruaddr1", "ruaddr2", "ruaddr3", "ruaddr4", "ruaddr5", "rupostcode", "tradstyle1", "tradstyle2","tradstyle3", "contact", "telephone", "fax", "seltype", "inclexcl", "cell_no", "formtype", "cso_tel", "currency"]` | list | A list of valid column names in the sample data. |
| sample_keep_columns | Sample columns to keep. | `["reference"]` | list | A list of valid column names in the sample data. |
| idbr_to_spp | Mapping between IDBR and SPP. | `{"201": 9, "202": 9, "203": 10, "204": 10, "205": 11, "216": 11, "106": 12, "111": 12, "117": 13, "167": 13, "123": 14, "173": 14, "817": 15, "867": 15, "823": 16, "873": 16}` | dict | A dictionary in the format `{"IDBR_value": SPP_value}` where IDBR value is a string and SPP value is an int. |
| census_extra_calibration_group | Census calibration groups that require separate handling. | `[5043, 5113, 5123, 5203, 5233, 5403, 5643, 5763, 5783, 5903, 6073]` | list | List of ints. |
| filter_out_questions | A list of questions to filter out when running the pipeline. | `[11, 12 , 146]` | list | A list of ints where each int refers to a question. |
| csw_to_spp_columns | Mapping of CSW to SPP columns. | `{"returned_value":"response", "adjusted_value":"adjustedresponse", "question_no":"questioncode"}` | dict | A dictionary in the format `{"CSW_col_name": "SPP_col_name"}`. |
| type_to_imputation_marker | A dictionary mapper mapping type to imputation marker. | `{"0": "r", "1": "r", "2": "derived", "3": "fir", "4": "bir", "5": "c", "6": "mc", "10": "r", "11": "r", "12": "derived", "13": "fir" }` | dict | A dictionary in the format `{"type":"imputation_marker"}` where imputation marker is a value found in the imputation_marker_col. |
| mandatory_outputs | A list of mandatory outputs to produce after the pipeline has run. | `["produce_qa_output", "turnover_output",               "growth_rates_output", "mbs_format_population_counts"]` | list | Any of the outputs listed in `mbs_results/outputs/produce_additional_outputs.py` within the `produce_additional_outputs` function which must be produced. |
| form_to_derived_map | A dictionary mapper mapping form type to question number for derived questions | `{"13": [40],"14": [40],"15": [46],"16": [42]}` | dict | A dictionary in the format `{"formtype":["question_no"]}` where each key-value pair represents the form type and question number for each derived question in the data. Note that question number is a list, even if there's only one. |
| devolved_questions | Questions to include in devolved outputs. | `[11, 12, 40, 49, 110]` | list | List of ints. |
| question_no_plaintext | Mapping of question numbers to human-readable names. | `{ "11": "start_date", ... }` | dict | Any mapping from question number to label. |
| local_unit_columns | Local unit column names for population outputs. | `["ruref", "entref", "lu ref", "check letter", ...]` | list | A list of valid column names. |
| nil_values | List of NIL status strings. | `["Combined child (NIL2)", "Out of scope (NIL3)", "Ceased trading (NIL4)" ,"Dormant (NIL5)", "Part year return (NIL8)", "No UK activity (NIL9)"]` | list | List of strings matching NIL statuses. |
| pounds_thousands_questions | Questions for which values are expressed in thousands of pounds. | `[40, 42, 43, 46, 47, 48, 49, 90]` | list | List of ints. |
| non_response_statuses | A list of status values that refer to non-responses. | `["Excluded from Results", "Form sent out"]` | list | A list of statuses found in the `status` column. |

## Updating
Please update this when you can - for example, if anything is added/removed from the config, or some of the sensible default values change, update this as part of your pull request.
