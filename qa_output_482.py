def get_qa_output_482(post_win_df):
    """For ASAP-482"""
    
    requested_columns = [
        'reference',"period","frosic2007",'classification','cell_no','frotover',
        'froempees', 'form_type', 'response_type', 'question_no','adjusted_value',
        'error_mkr', 'design_weight','calibration_factor', 'outlier_weight',
        'total weight (A*G*O)','weighted adjusted value', 'imputation_flags_adjusted_value',
        'imp_class','f_link_adjusted_value', 'f_match_adjusted_value_pair_count',
        'default_link_f_match_adjusted_value','b_link_adjusted_value',
        'b_match_adjusted_value_pair_count','default_link_b_match_adjusted_value',
        'construction_link', 'flag_construction_matches_pair_count','default_link_flag_construction_matches',
        "imputed_value","constrain_marker" # these not requested but usefull
        ]

    # not part of the pipeline the below
    post_win_df['total weight (A*G*O)'] = post_win_df['design_weight']*post_win_df['calibration_factor']*post_win_df['outlier_weight']
    
    post_win_df['weighted adjusted value'] =post_win_df['imputed_value'] * post_win_df['total weight (A*G*O)']


    return post_win_df[requested_columns]










