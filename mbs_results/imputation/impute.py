import warnings

import pandas as pd

from mbs_results.imputation.ratio_of_means import ratio_of_means
from mbs_results.staging.data_cleaning import (
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
            strata="imputation_class",
            auxiliary=config["auxiliary"],
        )
    )

    post_impute["period"] = post_impute["period"].dt.strftime("%Y%m").astype("int")
    post_impute = post_impute.reset_index(drop=True)  # remove groupby leftovers
    post_constrain = constrain(
        df=post_impute,
        period=config["period"],
        reference=config["reference"],
        target=config["target"],
        question_no=config["question_no"],
        spp_form_id="form_type_spp",
    )

    return post_constrain
