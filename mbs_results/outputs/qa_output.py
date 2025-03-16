from importlib import metadata

import pandas as pd


def save_intermediate_qa_output(
    dataframe: pd.DataFrame, config: dict, name: str
) -> None:
    """
    Function to create the intermediate QA output.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing the data to be used for the intermediate QA output
    config : dict
        Dictionary containing the configuration settings for the pipeline

    """
    file_version_mbs = metadata.metadata("monthly-business-survey-results")["version"]
    filename = f"{config['output_path']}{name}_v{file_version_mbs}.csv"
    print(filename)
    dataframe.to_csv(filename, index=False)
