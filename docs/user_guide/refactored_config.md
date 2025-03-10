# Refactored `config.json`

This refactoring of the `config.json` file is for the `Jira` ticket `733-review-config`. The approach adopted here ensures a clean separation between user-configurable settings and config_dev while dynamically constructing file paths.

As part of this update, the file input/output (I/O) handling has been refactored to support two modes of operation based on the environment: **Production (prod)** and **Development (dev)**.

* **Production (prod)** mode:  The code will read from and save to **AWS S3**.
* **Development (dev)** mode: The code will read from and save to the **local file system**.

This flexibility allows the **MBS-RESULTS** pipeline to be used in both environments by simply configuring the mode via a central configuration file, `config_user.json`.


Here is the structure:

## 1. Spliting `config.json`: into Two Files
* `config_user.json`: Contains variables that users can change.
* `constant.json`: Contains fixed values like DataFrame column names, data types and other config_dev (can only be changed for debugging and by the dev team).

## 2. Location of the New and Modified Files and Directory
* New Files and Directories
    * `/monthly-business-survey-results/mbs_results/config_user.json`
    * `/monthly-business-survey-results/mbs_results/config_dev.json`
    * `\monthly-business-survey-results\mbs_results\doc\docs/user_guide/refactored_config.md`
    * `/monthly-business-survey-results/mbs_results/utilities/load_file_object.py`
    * `/home/cdsw/monthly-business-survey-results/mbs_results/utilities/save_file.py`
    * `/monthly-business-survey-results/mbs_results/utilities/s3_operations_utils.py`
* Modified Files and Directories
    * `/monthly-business-survey-results/mbs_results/utilities/inputs.py`
    * `/home/cdsw/monthly-business-survey-results/mbs_results/__init__.py`
    * `/home/cdsw/monthly-business-survey-results/setup.cfg`


## 3. Practical Input and Output File Tree

/mbs_results_files/
├── /in/
│   ├── universe009_*
│   ├── finalsel009_*
│   ├── periodzero_009_202112.csv
│   └── cp_009_202112.csv
├── /out/
│   ├── processed_file1.csv
│   └── processed_file2.json
├── /mapping_files/
│   ├── cell_no_calibration_group_mapping.csv
│   ├── classification_sic_mapping.csv
│   ├── classification_question_number_l_value_mapping.csv
│   ├── sic_domain_mapping.csv
│   └── form_domain_threshold_mapping.csv
├── /mbs_anonymised/
│   ├── cp_009_202112.csv
└── /release/
    ├── finalsel009_202112
    ├── manual_constructions.csv
    └── snapshot_qv_cp_202303_15.json


## 4. Modified `input.py` to Combine and Manage Configurations
* Reads the `config_user.json` and  `config_dev.json` JSON files.
* Constructs file paths dynamically.
* Merges them into a single configuration dictionary, `config`
* Connect the single configuration dictionary to the rest of the codes

## 5. Modified `setup.cfg`
Added `from_root` to the list of `install_requires`

## 6. Modified `__init__.py`
Created a custom logger `MBS`. To access and use the `logger` add the part as:
`from mbs_results import logger`

## 7. File I/O Refactor
The behaviour of the I/O is controlled by a `config`. It has the following key values:

* `platform`: Specifies where the files should be read from and written to. It can have two possible values:
    * `s3`: Use **AWS S3** for the file operations (for prod mode).
    * `network`: Use the **local file system** (for dev mode).
        * set `"local_drive_directory": "./"` on linux (cdsw) or "local_drive_directory": "D:/" on Windows PC
* `s3_bucket`: The name of the S3 bucket (required if `platform` is `s3`).

## 8. Config.json Examples
* Store files in S3 Bucket
    * `"platform": "s3"`,
    * `"s3_bucket": "onscdp-dev-data01-5320d6ca"`
    * `"parent_path": "bat/mbs_results_files/"`,
* Store files in local directory (network)
    * `"platform": "network"`,
    * `"local_drive_directory": "./"`
    * `"parent_path": "mbs_results_files/"`,
* Store files in local directory (PC)
    * `"platform": "network"`,
    * `"local_drive_directory": "D:/"`
    * `"parent_path": "mbs_results_files/"`,
