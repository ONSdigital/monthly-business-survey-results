import logging

import pandas as pd

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import produce_additional_outputs
from mbs_results.staging.back_data import read_and_process_back_data
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    create_imputation_class,
)
from mbs_results.staging.stage_dataframe import drop_derived_questions
from mbs_results.utilities.constrains import constrain
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import qa_selective_editing_outputs

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="period_zero_se_wrapper.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def period_zero_se_wrapper():
    """
    wrapper for selective editing outputs when using period zero data supplied from csw
    function does some minor imputation processing to update cell_number and imputation
    class using new idbr data.
    Estimation and outlier detection is then run on the processed data.
    Selective editing question and contributor files are then produced

    """

    config = load_config(path="./mbs_results/config.json")

    # Read in back data
    back_data = read_and_process_back_data(config)
    # back_data[config["auxiliary"]+"_original"] = back_data[onfig["auxiliary"]].copy()
    back_data[config["auxiliary_converted"]] = back_data[config["auxiliary"]].copy()
    back_data = convert_annual_thousands(back_data, config["auxiliary_converted"])

    # Lots of "glueing" other functions together so df is in a format for estimation
    # Another method could be to look at imputation, and try to keep original imputation
    # markers.

    # Have to drop derived questions in this way, other method dropped "derived"
    # imputation markers, these are needed because a reference can change form type
    # i.e. have 46 as derived in one period, then 40 derived in next period and 46 used
    # to derive 40

    back_data = drop_derived_questions(
        back_data,
        config["question_no"],
        config["form_id_spp"],
    )

    back_data_imputation = imputation_processing(back_data, config)

    # Running all of estimation and outliers
    back_data_estimation = estimate(back_data_imputation, config)

    back_data_outliering = detect_outlier(back_data_estimation, config)

    additional_outputs_df = back_data_estimation[
        [
            config["reference"],
            config["period"],
            config["design_weight"],
            "frosic2007",
            config["form_id_idbr"],
            config["question_no"],
            config["auxiliary_converted"],
            config["calibration_factor"],
            config["target"],
            "response",
            "froempment",
            config["cell_number"],
            "imputation_class",
            "imputed_and_derived_flag",
            "construction_link",
        ]
    ]
    additional_outputs_df.rename(
        columns={"imputed_and_derived_flag": "imputation_flags_adjustedresponse"},
        inplace=True,
    )

    additional_outputs_df = additional_outputs_df.merge(
        back_data_outliering[["reference", "period", "questioncode", "outlier_weight"]],
        how="left",
        on=["reference", "period", "questioncode"],
    )

    additional_outputs_df["formtype"] = additional_outputs_df["formtype"].astype(str)

    additional_outputs_df.drop_duplicates(inplace=True)

    produce_additional_outputs(config, additional_outputs_df)

    qa_selective_editing_outputs(config)


def imputation_processing(back_data: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Processing that should have been done during imputation.
    For period zero data, we do not do this processing, therefore we need to run the
    specific functions needed for estimation and outliering here

    Parameters
    ----------
    back_data : pd.DataFrame
        dataframe consisting only of back data i.e. period zero data
    config : dict
        main pipeline config dictionary

    Returns
    -------
    pd.DataFrame
        processed back data dataframe with derived questions, imputation class and
        construction link.
    """

    back_data = constrain(
        df=back_data,
        period=config["period"],
        reference=config["reference"],
        target=config["target"],
        question_no=config["question_no"],
        spp_form_id=config["form_id_spp"],
    )

    back_data["imputed_and_derived_flag"] = back_data.apply(
        lambda row: (
            "d"
            if "sum" in str(row["constrain_marker"]).lower()
            else row[f"imputation_flags_{config['target']}"]
        ),
        axis=1,
    )

    back_data = create_imputation_class(
        back_data, config["cell_number"], "imputation_class"
    )

    # Run apply_imputation_link function to get construction links
    back_data_cons_matches = flag_construction_matches(back_data, **config)

    # group by question number then apply this function
    back_data_imputation = back_data_cons_matches.groupby(config["question_no"]).apply(
        lambda df: calculate_imputation_link(
            df,
            match_col="flag_construction_matches",
            link_col="construction_link",
            predictive_variable=config["auxiliary_converted"],
            **config,
        )
    )

    # Changing period back into int. Read_colon_sep_file should be updated to enforce
    # data types as per the config.

    back_data_imputation["period"] = (
        back_data_imputation["period"].dt.strftime("%Y%m").astype("int")
    )
    back_data_imputation["frosic2007"] = back_data_imputation["frosic2007"].astype(
        "str"
    )

    return back_data_imputation


if __name__ == "__main__":
    print("wrapper start")
    period_zero_se_wrapper()
    print("wrapper end")
