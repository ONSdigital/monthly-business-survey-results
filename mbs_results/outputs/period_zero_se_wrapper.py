import logging

import pandas as pd

from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.imputation.flag_and_count_matched_pairs import count_matches
from mbs_results.outputs.selective_editing_outputs import create_se_outputs
from mbs_results.staging.back_data import read_and_process_back_data
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    convert_cell_number,
    create_imputation_class,
)
from mbs_results.staging.stage_dataframe import drop_derived_questions
from mbs_results.utilities.inputs import load_config

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
    # Issues:
    # - Loading p1 IDBR files, which are used to calculate construction links?
    # Double check which periods frotover and cell number should be used
    # for construction links.

    config = load_config(None)

    # Read in back data
    back_data = read_and_process_back_data(config)

    # Have to drop derived questions in this way, other method dropped "derived"
    # imputation markers, these are needed because a reference can change form type
    # i.e. have 46 as derived in one period, then 40 derived in next period and 46 used
    # to derive 40

    back_data = drop_derived_questions(
        back_data,
        config["question_no"],
        config["form_id_spp"],
        config["form_to_derived_map"],
    )
    back_data_imputation = imputation_processing(back_data, config)

    back_data_imputation.to_csv(
        config["output_path"] + "back_data_imputation.csv", index=False
    )

    create_se_outputs(back_data_imputation, config)


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
        main pipeline config dictionary, currently using SIC

    Returns
    -------
    pd.DataFrame
        processed back data dataframe with derived questions, imputation class and
        construction link.
    """

    # Convert Aux to £'s monthly
    back_data[config["auxiliary_converted"]] = back_data[config["auxiliary"]].copy()
    back_data = convert_annual_thousands(back_data, config["auxiliary_converted"])

    # Convert cell number to not include NI
    back_data = convert_cell_number(back_data, config["cell_number"])
    back_data = create_imputation_class(
        back_data, config["cell_number"], "imputation_class"
    )

    # Run apply_imputation_link function to get construction links
    back_data_cons_matches = (
        back_data.groupby(config["question_no"])
        .apply(lambda df: flag_construction_matches(df, **config))
        .reset_index(drop=True)
    )

    back_data_cons_matches = (
        back_data_cons_matches.groupby(config["question_no"])
        .apply(
            lambda df: count_matches(
                df,
                flag="flag_construction_matches",
                period=config["period"],
                strata="imputation_class",
            )
        )
        .reset_index(drop=True)
    )

    # group by question number then apply this function
    back_data_imputation = (
        back_data_cons_matches.groupby(config["question_no"])
        .apply(
            lambda df: calculate_imputation_link(
                df,
                match_col="flag_construction_matches",
                link_col="construction_link",
                predictive_variable=config["auxiliary_converted"],
                strata="imputation_class",
                target=config["target"],
                period=config["period"],
            )
        )
        .reset_index(drop=True)
    )

    # Changing period back into int. Read_colon_sep_file should be updated to enforce
    # data types as per the config.

    back_data_imputation["period"] = (
        back_data_imputation["period"].dt.strftime("%Y%m").astype("int")
    )
    back_data_imputation[config["sic"]] = back_data_imputation[config["sic"]].astype(
        "str"
    )

    return back_data_imputation


if __name__ == "__main__":
    print("wrapper start")
    period_zero_se_wrapper()
    print("wrapper end")
