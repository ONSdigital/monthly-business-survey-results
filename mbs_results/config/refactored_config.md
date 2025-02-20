# Refactored `config.json`

This refactoring of the `config.json` file is for the `Jira` ticket `733-review-config`. The approach adopted here ensures a clean separation between user-configurable settings and constants while dynamically constructing file paths. Here is the structure:

## 1. Spliting `config.json`: into Two Files
* `user_config.json`: Contains Variables that users can change.
* `constant.json`: Contains fixed values like DataFrame column names, data types and other constants (can only be changed for debugging and by the dev team).

## 2. `configuration.py` to Combine and Manage Configurations
* Reads both JSON files.
* Constructs file paths dynamically.
* Merges them into a single configuration dictionary
* Connect the single configuration dictionary to the rest of the codes

## 3. Location of the New Files and Directory
* `\monthly-business-survey-results\mbs_results\config\`
* `\monthly-business-survey-results\mbs_results\user_config.json`
* `\monthly-business-survey-results\mbs_results\config\constant.json`
* `\monthly-business-survey-results\mbs_results\config\configuration.py`