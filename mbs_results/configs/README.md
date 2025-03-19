# User Config

| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| bucket | The path to the bucket. | string | Any filepath. |
| calibration_group_map_path | The filepath to the calibration group mapping file. | string | Any filepath. |
| classification_values_path | The filepath to the file containing SIC classification values. | string | Any filepath. |
| folder_path | The path to the folder containing input data. | string | Any filepath. |
| l_values_path | The filepath to the file containing l values. | string | Any filepath. |
| manual_constructions_path | The filepath to the file containing manual constructions data. | string | Any filepath. |
| mbs_file_name | The filepath to the input data. | string | Any filepath. |
| output_path | The filepath where outputs should be saved to. | string | Any filepath. |
| population_path | The filepath to the file containing population frame data. | string | Any filepath. |
| sample_path | The filepath for the file containing sample data. | string | Any filepath. |
| back_data_qv_path | The filepath for the file containing QV backdata. | string | Any filepath. |
| back_data_cp_path | The filepath for the file containing CP backdata. | string | Any filepath. |
| back_data_finalsel_path | The filepath for the file containing final selection backdata. | string | Any filepath. |
| sic_domain_mapping_path | The filepath for the data containing the mapping from SIC codes to domains. | string | Any filepath. |
| threshold_filepath | The filepath for the data containing thresholds for selective editing. | string | Any filepath. |
| period_selected | The most recent period to include in the outputs. | int | Any int in the form `yyyymm`. |
| current_period | The most recent period to include in the outputs (same as above). | int | Any int in the form `yyyymm`. |
| previous_period | The previous period to use as a reference | int | Any int in the form `yyyymm`. |
| revision_period | The number of months to use as a revision period. | int | Any int in the form `mm` or `m` (does not need to be zero-padded). |


## Guidance for use
As an end user, you will only need to change the user config (named `config_user.json`) - you just need to update the filepaths and period information in the user config. Note: for ONS users, you can find example filepaths in the Confluence documentation.

# Dev Config
| Parameter | Description | Default | Data Type | Acceptable Values |
|---|---|---|---|---|
| platform | Specifies whether you're running the pipeline locally or on DAP. | `"s3"` | string | `"network"`, `"s3"` |
| back_data_type | The name of the backdata type marker column. | `"type"` | string | Any valid column name. |
| imputation_marker_col | The name of the column being used as an imputation marker. | `"imputation_flags_adjustedresponse"` | string | Any valid column name. |
| auxiliary | The name of the column containing the auxiliary variable. | `"frotover"` | string | Any valid column name. |
| auxiliary_converted | The name of the column containing the auxiliary variable converted into monthly actual pounds. | `"converted_frotover"` | string | Any valid column name. |
| calibration_factor | The name of the column containing the calibration factor variable. | `"calibration_factor"` | string | Any valid column name. |
| cell_number | The name of the column containing the cell number variable. | `"cell_no"` | string | Any valid column name. |
| design_weight | The name of the column containing the design weight variable. | `"design_weight"` | string | Any valid column name. |
| status | The name of the column containing the status variable. | `"statusencoded"` | string | Any valid column name. |
| form_id_idbr | The name of the column containing the form type (IDBR) variable. | `"formtype"` | string | Any valid column name. |
| group | The name of the column containing the group variable. | `"calibration_group"` | string | Any valid column name. |
| calibration_group | The name of the column containing the calibration group variable. | `"calibration_group"` | string | Any valid column name. |
| period | The name of the column containing the period variable. | `"period"` | string | Any valid column name. |
| question_no | The name of the column containing the question number/code variable. | `"questioncode"` | string | Any valid column name. |
| reference | The name of the column containing the reference variable. | `"reference"` | string | Any valid column name. |
| region | The name of the column containing the region variable. | `"region"` | string | Any valid column name. |
| sampled | The name of the column containing the is_sampled variable. | `"is_sampled"` | string | Any valid column name. |
| census | The name of the column containing the is_census variable. | `"is_census"` | string | Any valid column name. |
| state | The name of the column containing the state variable. | `"frozen"` | string | Any valid column name. |
| strata | The name of the column containing the strata variable. | `"cell_no"` | string | Any valid column name. |
| target | The name of the column containing the target variable. | `"adjustedresponse"` | string | Any valid column name. |
| form_id_spp | The name of the column containing the form type (SPP) variable. | `"form_type_spp"` | string | Any valid column name. |
| master_column_type_dict | Defines the expected data types for various columns. | `{"reference": "int", "period": "date", "response": "str", "questioncode": "int", "adjustedresponse": "float", "frozensic": "str", "frozenemployees": "int", "frozenturnover": "float", "cellnumber": "int", "formtype": "str", "status": "str", "statusencoded": "int", "frosic2007": "str", "froempment": "int", "frotover": "float", "cell_no": "int"}` | dict | Any dictionary in the format `{"column_name": "data_type"}` where column name is a valid column and data_type is one of `"bool"`, `"int"`, `"str"` or `"float"`. Both key and value should be enclosed in quotation marks. |
| contributors_keep_cols | Columns to keep for contributors. | `["period", "reference", "status", "statusencoded"]` | list | A list of valid column names. |
| responses_keep_cols | Columns to keep for responses. | `["adjustedresponse", "period", "questioncode", "reference", "response"]` | list | A list of valid column names. |
| finalsel_keep_cols | Columns to keep for final selection. | `["formtype", "cell_no", "froempment", "frosic2007", "frotover", "period", "reference"]` | list | A list of valid column names. |
| temporarily_remove_cols | Columns to temporarily remove. | [] | list | A list of valid column names. |
| non_sampled_strata | Non-sampled strata values. | `["5141", "5142", "5143", "5371", "5372", "5373", "5661", "5662", "5663"]` | list | A list of cell numbers/strata. |
| population_column_names | Column names in the population frame. | `["reference", "checkletter", "inqcode", "entref", "wowentref", "frosic2003", "rusic2003", "frosic2007", "rusic2007", "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", "frotover", "turnover", "entrepmkr", "legalstatus", "inqstop", "entzonemkr", "region", "live_lu", "live_vat", "live_paye", "immfoc", "ultfoc", "cell_no", "selmkr", "inclexcl" ]` | list | A list of valid column names in the population frame dataset. |
| population_keep_columns | Population columns to keep. | `["reference", "region", "frotover", "cell_no", "period", "frosic2007"]` | list | A list of valid column names in the population frame dataset. |
| sample_column_names | Column names in the sample data. | `["reference", "checkletter", "frosic2003", "rusic2003", "frosic2007", "rusic2007", "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", "frotover", "turnover", "entref", "wowentref", "vatref", "payeref", "crn", "live_lu", "live_vat", "live_paye","legalstatus", "entrepmkr", "region", "birthdate", "entname1", "entname2", "entname3", "runame1", "runame2","runame3", "ruaddr1", "ruaddr2", "ruaddr3", "ruaddr4", "ruaddr5", "rupostcode", "tradstyle1", "tradstyle2","tradstyle3", "contact", "telephone", "fax", "seltype", "inclexcl", "cell_no", "formtype", "cso_tel", "currency"]` | list | A list of valid column names in the sample data. |
| sample_keep_columns | Sample columns to keep. | `["reference", "period"]` | list | A list of valid column names in the sample data. |
| filter_out_questions | A list of questions to filter out when running the pipeline. | `[11, 12 , 146]` | list | A list of ints where each int refers to a question. |
| idbr_to_spp | Mapping between IDBR and SPP. | `{"201": 9, "202": 9, "203": 10, "204": 10, "205": 11, "216": 11, "106": 12, "111": 12, "117": 13, "167": 13, "123": 14, "173": 14, "817": 15, "867": 15, "823": 16, "873": 16}` | dict | A dictionary in the format `{"IDBR_value": SPP_value}` where IDBR value is a string and SPP value is an int. |
| csw_to_spp_columns | Mapping of CSW to SPP columns. | `{"returned_value":"response", "adjusted_value":"adjustedresponse", "question_no":"questioncode"}` | dict | A dictionary in the format `{"CSW_col_name": "SPP_col_name"}`. |
| type_to_imputation_marker | A dictionary mapper mapping type to imputation marker. | `{"0": "r", "1": "r", "2": "derived", "3": "fir", "4": "bir", "5": "c", "6": "mc", "10": "r", "11": "r", "12": "derived", "13": "fir" }` | dict | A dictionary in the format `{"type":"imputation_marker"}` where imputation marker is a value found in the imputation_marker_col. |
| additional_outputs | A list of additional outputs to produce after the pipeline has run. | [] | list | Any of the additional outputs listed in `mbs_results/outputs/produce_additional_outputs.py` within the `produce_additional_outputs` function. See below for information on how to generate additional outputs. Currently: `"selective_editing_contributor"`, `"selective_editing_question"`, `"turnover_output"`, `"weighted_adj_val_time_series"`, `"produce_ocea_srs_outputs"`, `"create_imputation_link_output"` or `["all"]` to produce all additional outputs. |
## Usage


**Adding new columns**: To add new columns throughout the pipeline, you will need to add it to one of the keep_cols, i.e. `finalsel_keep_cols`, `responses_keep_cols` or `contributors_keep_cols` **and** you will also need to add it to the `master_column_type_dict` parameter.


**Generating additional outputs**: To generate additional outputs, as well as adding the name of the additional output to the `additional_outputs` parameter, you will also need to ensure the additional output is listed in the dictionary [here](https://github.com/ONSdigital/monthly-business-survey-results/blob/main/mbs_results/outputs/produce_additional_outputs.py#L92) in the format `{"output_name": function_to_produce_output}` (ensuring that you've imported `function_to_produce_output` at the top of the script.)

## Updating
Please update this when you can - for example, if anything is added/removed from the config, or some of the sensible default values change, update this as part of your pull request.
