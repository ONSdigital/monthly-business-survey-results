# Old module structure
mbs_results/
├── apply_estimation.py
├── apply_imputation_link.py
├── blank.py
├── calculate_estimation_weights.py
├── calculate_imputation_link.py
├── calculate_predicted_unit_value.py
├── calculate_ratio_estimation.py
├── calculate_winsorised_weight.py
├── constrains.py
├── construction_matches.py
├── cumulative_imputation_links.py
├── data_cleaning.py
├── flag_and_count_matched_pairs.py
├── flag_for_winsorisation.py
├── growth_rate_output.py
├── imputation_flags.py
├── inputs.py
├── link_filter.py
├── merge_domain.py
├── pivot_imputation_value.py
├── predictive_variable.py
├── pre_processing_estimation.py
├── ratio_of_means.py
├── README.md
├── replace_l_values.py
├── selective_editing.py
├── turnover_analysis.py
├── utils.py
├── validate_estimation.py
├── validate_imputation.py
├── validation_checks.py
├── winsorisation.py
└── `__init__.py`

# New Proposed Module Structure

mbs_results/
├── `__init__.py`
├── README.md
├── imputation/
│   ├── `__init__.py`
│   ├── calculate_imputation_link.py
│   ├── cumulative_imputation_links.py
│   ├── flag_and_count_matched_pairs.py
│   ├── imputation_flags.pyk
│   ├── link_filter.py          #1 function but 2 TODOs
│   ├── pivot_imputation_value.py
│   ├── predictive_variable.py
│   ├── ratio_of_means.py       # alot of commented out lines and TODOs
│   └── validate_imputation.py
├── estimation/
│   ├── `__init__.py`
│   ├── apply_imputation_link.py    #  Alot of inconsistant comments and TODOs
│   ├── calculate_estimation_weights.py
│   ├── pre_processing_estimation.py
│   └── validate_estimation.py      #  line 49/50 comments
├── construction/
│   ├── `__init__.py`
│   └── construction_matches.py     # moved to imputation
├── outlier_detection/
│   ├── `__init__.py`
│   ├── calculate_predicted_unit_value.py
│   ├── calculate_ratio_estimation.py
│   ├── calculate_winsorised_weight.py
│   ├── flag_for_winsorisation.py
│   ├── replace_l_values.py
│   └── winsorisation.py
├── outputs/ - *New Proposed Change, move outputs from branch to main package*
│   ├── `__init__.py`
│   └── growth_rate_output.py
├── utilities/
│   ├── utils.py
│   ├── data_cleaning.py    # Alot of comments and functions, potential for cleaning up
│   └── inputs.py
└── unsorted/
    ├── constrains.py - applied after imputation?   #Alot of comments including operation changes
    ├── selective_editing.py?
    ├── turnover_analysis.py    #Comment on lines 70-72 seems important?
    └── validation_checks.py

test/
|   test_imputation/
│   ├── `__init__.py`
│   ├── calculate_imputation_link.py
│   ├── cumulative_imputation_links.py
│   ├── flag_and_count_matched_pairs.py
│   ├── imputation_flags.py
│   ├── link_filter.py
│   ├── pivot_imputation_value.py
│   ├── predictive_variable.py
│   ├── ratio_of_means.py
│   └── validate_imputation.py
