import pandas as pd

def produce_qa_output(config: dict, post_win_df: pd.DataFrame) -> pd.DataFrame:
    
    """Produces an output with required columns, and with total weight and weighted adjusted value calculated.
     
     Parameters
    ----------
    config : dict
        The config as a dictionary.
    post_win_df : pd.DataFrame
        Dataframe containing the required columns, following the outlier_detection module.
        """
    requested_columns = [
        'reference',
        'period',
        'sic',
        'classification',
        'cell_no',
        'frotover',
        'froempment', 
        'form_type', 
        'response_type', 
        'question_no',
        'adjusted_value',
        'error_mkr', 
        'design_weight',
        'calibration_factor', 
        'outlier_weight',
        'total weight (A*G*O)',
        'weighted adjusted value', 
        'imputation_flags_adjusted_value',
        'imputation_class',
        'f_link_adjusted_value', 
        'f_match_adjusted_value_pair_count',
        'default_link_f_match_adjusted_value',
        'b_link_adjusted_value',
        'b_match_adjusted_value_pair_count',
        'default_link_b_match_adjusted_value',
        'construction_link', 
        'flag_construction_matches_pair_count',
        'default_link_flag_construction_matches',
        'constrain_marker' # these not requested but usefull
        ]
    
    # Check if column names specified in config, if not, use above as default
    cols_from_config = []

    for col in requested_columns:
        cols_from_config.append(config.get(col, col))

    post_win_df['total weight (A*G*O)'] = post_win_df['design_weight'] * post_win_df['calibration_factor'] * post_win_df['outlier_weight']
    
    post_win_df['weighted adjusted value'] = post_win_df['adjusted_value'] * post_win_df['total_weight_(A*G*O)']

    return post_win_df[cols_from_config]