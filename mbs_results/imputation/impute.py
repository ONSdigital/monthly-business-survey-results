import warnings

import pandas as pd

from mbs_results.imputation.ratio_of_means import ratio_of_means
from mbs_results.staging.data_cleaning import (
    convert_cell_number,
    create_imputation_class,
    load_manual_constructions,
)
from mbs_results.utilities.constrains import constrain


def impute(dataframe: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    wrapper function to apply imputation to the given dataframe

    Parameters
    ----------
    dataframe : pd.DataFrame
        dataframe with both contributors and responses from snapshot
    config : dict
        config file containing column names and manual construction path

    Returns
    -------
    pd.DataFrame
        post imputation dataframe, values have been derived and constrained following
        imputation
    """
    warnings.warn("Check what will happen if we try and apply RoM to q146 - Comments")
    # If this is an issue, we could filter to remove 146 and
    # add back after, or escape from Rom if q==146...
    dataframe["ni_gb_cell_number"] = dataframe[config["cell_number"]]

    dataframe = convert_cell_number(dataframe, config["cell_number"])

    pre_impute_dataframe = create_imputation_class(
        dataframe, config["cell_number"], "imputation_class"
    )

    # Two options for loading MC:
    warnings.warn("Need to pick one method of loading manual constructions")
    try:
        manual_constructions = pd.read_csv(config["manual_constructions_path"])
        if manual_constructions.empty:
            manual_constructions = None
        # We could implement above, or the other method of loading mc:
        load_manual_constructions(df=pre_impute_dataframe, **config)
    except FileNotFoundError:
        manual_constructions = None
    post_impute = pre_impute_dataframe.groupby(config["question_no"]).apply(
        lambda df: ratio_of_means(
            df=df,
            manual_constructions=manual_constructions,
            reference=config["reference"],
            target=config["target"],
            period=config["period"],
            current_period=config["current_period"],
            revision_period=config["revision_period"],
            question_no=config["question_no"],
            strata="imputation_class",
            auxiliary=config["auxiliary_converted"],
        )
    )

    post_impute["period"] = post_impute["period"].dt.strftime("%Y%m").astype("int")
    post_impute = post_impute.reset_index(drop=True)  # remove groupby leftovers
    post_impute = post_impute[~post_impute["is_backdata"]]  # remove backdata
    post_impute.drop(columns=["is_backdata"], inplace=True)

    post_constrain = constrain(
        df=post_impute,
        period=config["period"],
        reference=config["reference"],
        target=config["target"],
        question_no=config["question_no"],
        spp_form_id=config["form_id_spp"],
    )

    post_constrain["imputed_and_derived_flag"] = post_constrain.apply(
        lambda row: (
            "d"
            if "sum" in str(row["constrain_marker"]).lower()
            else row[f"imputation_flags_{config['target']}"]
        ),
        axis=1,
    )

    # Added reverse mapping for idbr formtype. Needed for SE and other outputs
    spp_to_idbr_mapping = {value: key for key, value in config["idbr_to_spp"].items()}
    post_constrain.loc[post_constrain["formtype"].isnull(), "formtype"] = (
        post_constrain.loc[post_constrain["formtype"].isnull(), "form_type_spp"].map(
            spp_to_idbr_mapping
        )
    )

    return post_constrain
