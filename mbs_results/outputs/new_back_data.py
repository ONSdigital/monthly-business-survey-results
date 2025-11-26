import pandas as pd

from mbs_results.utilities.outputs import write_json_wrapper


def get_backdata_from_period(
    df: pd.DataFrame, backdata_period: int, config: dict
) -> dict:
    """
        Filters df based on backdata_period and creates a dictionary with
    contributors and responses data. This format aims to replicate the SPP backdata
    format.

        Parameters
        ----------
        df : pd.Dataframe
            Original dataframe in which the backdata will be generated.
            It should be any dataframe which contains the variables generated
            from the imputation method.
        backdata_period : int
            The desired backdata period.
        config : dict
            The pipeline configuration.

        Returns
        -------
        output : dict
            A dictionary containing contributors and responses for the new
            back data period.
    """

    nas_or_derived_mask = (df[config["imputation_marker_col"]].isna()) | (
        df[config["imputation_marker_col"]] == "d"
    )

    df = df[~nas_or_derived_mask]

    backdata_contributors = df[
        [
            config["reference"],
            config["period"],
            config["status"],
            config["nil_status_col"],
        ]
    ]

    # Hard coding response variable name, is not needed but keeping it
    # to match current back data format

    backdata_responses = df[
        [
            config["reference"],
            config["period"],
            config["question_no"],
            "response",
            config["target"],
            config["imputation_marker_col"],
        ]
    ]

    backdata_responses = backdata_responses[
        backdata_responses["period"] == backdata_period
    ]

    backdata_contributors = backdata_contributors[
        backdata_contributors[config["period"]] == backdata_period
    ]

    # backdata_contributors has no questioncode so now it will have duplicates
    backdata_contributors = backdata_contributors.drop_duplicates()

    backdata_responses = backdata_responses.rename(
        columns={config["imputation_marker_col"]: "imputationmarker"}
    )

    output = {
        "snapshot_id": "{id}_{period}_backdata".format(
            id=config["run_id"], period=backdata_period
        ),
        "contributors": backdata_contributors.to_dict("list"),
        "responses": backdata_responses.to_dict("list"),
    }

    return output


def export_backdata(df: pd.DataFrame, config: dict):
    """A wrapper function to produce new back data.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe in which the backdata will be generated.
        It should be any dataframe which contains the variables generated
        from the imputation method.
    config : dict
        The pipeline configuration.
    Returns
    -------
    None.

    """
    # period is float here so will create YYYYMM.0 enforcing int

    df[config["period"]] = df[config["period"]].astype(int)

    min_period = min(df[config["period"]])

    run_id = config["run_id"]

    json_backdata = get_backdata_from_period(df, min_period, config)

    filename = "back_data_{min_period}_{run_id}.json".format(
        min_period=min_period, run_id=run_id
    )

    write_json_wrapper(
        json_data=json_backdata,
        file_name=filename,
        save_path=config["output_path"],
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )
