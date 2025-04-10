import pandas as pd

from mbs_results.utilities.inputs import load_config


def produce_ocea_srs_outputs(additional_outputs_df: pd.DataFrame,config: dict) -> pd.DataFrame:
    """Produces outputs for OCEA/SRS, pulling SIC from the config

    Parameters
      ----------
      additional_outputs_df
        Dataframe containing MBS data for creating additional outputs

      Returns
      -------
      output_df
          MBS output formatted according to SRS/OCEA requirements
    """
    
    output_df = additional_outputs_df[
        [
            "reference",
            "period",
            config["sic"],
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
            config["sic"]: "SIC",
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
