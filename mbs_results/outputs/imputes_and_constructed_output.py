import pandas as pd


def get_imputes_and_constructed_output(
    additional_outputs_df: pd.DataFrame, **config
) -> pd.DataFrame:
    """
    Creates imputes and constructed output for frozen runs only

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        The input DataFrame containing additional outputs.
    **config : dict
        A dictionary containing configuration parameters. Must include:
        - "state" (str): The state of the process, must be "frozen" to proceed.
        - "period" (str): The column name for the period.
        - "reference" (str): The column name for the reference.
        - "question_no" (str): The column name for the question number.
        - "target" (str): The column name for the target variable.

    Returns
    -------
    pd.DataFrame
        The imputes and constructed output DataFrame.
        Returns None if the "state" in the configuration is not "frozen".
    """
    if config["state"] != "frozen":
        return

    imputes_and_constructed_output = additional_outputs_df[
        [
            config["period"],
            config["reference"],
            config["question_no"],
            config["target"],
            "imputation_flags_adjustedresponse",
        ]
    ]

    imputes_and_constructed_output = imputes_and_constructed_output[
        ~imputes_and_constructed_output[
            "imputation_flags_adjustedresponse"
        ].str.contains("r", na=False)
    ]

    imputes_and_constructed_output = imputes_and_constructed_output.rename(
        columns={
            "adjustedresponse": "constructedresponse",
            "imputation_flags_adjustedresponse": "imputationmarker",
        }
    )

    return imputes_and_constructed_output
