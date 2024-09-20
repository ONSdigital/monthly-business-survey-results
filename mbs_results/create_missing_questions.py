import pandas as pd

def create_missing_questions(
    contributors_df:pd.DataFrame, 
    responses_df:pd.DataFrame, 
    reference:str, 
    period:str, 
    formid:str, 
    question_no:str, 
    mapper:dict
)-> pd.DataFrame:
    """
    

    Parameters
    ----------
    contributors_df : pd.DataFrame
        DESCRIPTION.
    responders_df : pd.DataFrame
        DESCRIPTION.
    reference : str
        DESCRIPTION.
    period : str
        DESCRIPTION.
    formid : str
        DESCRIPTION.
    question_no : str
        DESCRIPTION.
    mapper : dict
        DESCRIPTION.

    Returns
    -------
    None.

    """
   

    responses_df = responses_df.set_index([reference,period,formid,question_no])

    expected_responses_idx = (contributors_df
         .filter([reference,period,formid]) # Select needed fields
         .assign(**{question_no: contributors_df[formid].map(mapper)}) #Create new column with list of questions as value
         .explode(question_no) #Convert questions to rows
         .set_index([reference,period,formid,question_no]) 
         ).index
    
    responses_idx = responses_df.index
    missing_responses_idx = expected_responses_idx.difference(responses_idx)

    responses_reindexed = responses_df.reindex(responses_df.index.union(missing_responses_idx))

    return responses_reindexed.reset_index()






