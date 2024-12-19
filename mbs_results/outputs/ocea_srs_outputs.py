import pandas as pd


def produce_ocea_srs_outputs(additional_outputs_df: pd.DataFrame) -> pd.DataFrame:
    """Produces outputs for OCEA/SRS

    Parameters
      ----------
      additional_outputs_df
        Dataframe containing MBS data for creating additional outputs

      Returns
      -------
      output_df
          MBS output formatted according to SRS/OCEA requirements
    """

    output_df = df[
        [
            "reference",
            "period",
            "frosic2007",
            "response",
            "adjustedresponse",
            "outlier_weight",
            "calibration_factor",
            "design_weight",
            "frotover",
            "froempment",
        ]
    ]

    output_df = output_df.rename(
        {
            "reference": "Ruref",
            "period": "Period",
            "frosic2007": "SIC",
            "response": "return",
            "adjustedresponse": "adjusted",
            "calibration_factor": "gweight",
            "design_weight": "a-weight",
            "frotover": "registered_turnover",
            "froempment": "registered_employment",
        },
        axis=1,
    )

    return output_df
