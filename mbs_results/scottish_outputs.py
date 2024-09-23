loaded_config = {
    "scottish_outputs": [
        "period",
        "SUT",
        "cell",
        "RU",
        "name",
        "enterprise group",
        "SIC",
        "form type",
        "status",
        "%scottish",
        "frozen employment",
        "band",
        "response type",
        "error marker",
        "start date",
        "end date",
        "returned turnover",
        "adjusted turnover",
        "response type.1",
        "error marker.1",
        "returned to exports",
        "adjused to exports",
        "response type.2",
        "error marker.2",
        "returned total employment",
        "adjusted total employment",
        "response type.3",
        "error marker.3",
        "returned FTM",
        "adjusted FTM",
        "response type.4",
        "error marker.4",
        "returned PTM",
        "adjusted PTM",
        "response type.5",
        "error marker.5",
        "returned FTF",
        "adjusted FTF",
        "response type.6",
        "error marker.6",
        "returned PTF",
        "adjusted PTF",
        "response type.7",
        "error marker.7",
        "returned volume water",
        "adjusted volume water",
        "response type.8",
        "error marker.8",
    ]
}

import pandas as pd

df = pd.read_csv("d:/temp_outputs_new_env/winsorisation_output_0.0.2.csv")
df = df.columns.to_list()
print(df)


def scottish_outputs(df: pd.DataFrame, scotish_columns: list, sup_data: pd.DataFrame):
    """
    Function to produce Scottish (and Welsh?) outputs
    Some data is not available from only MBS, do we need to request QBS data,
    is this for us to do or out of scope?

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    scotish_columns : list
        _description_
    sup_data : pd.DataFrame
        _description_
    """
    df[scotish_columns]
