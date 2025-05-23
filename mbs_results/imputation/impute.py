import pandas as pd

from mbs_results.imputation.ratio_of_means import ratio_of_means
from mbs_results.utilities.constrains import constrain


def impute(
    dataframe: pd.DataFrame, manual_constructions, config: dict, filter_df=None
) -> pd.DataFrame:
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
    post_impute = dataframe.groupby(config["question_no"]).apply(
        lambda df: ratio_of_means(
            df=df,
            manual_constructions=manual_constructions,
            reference=config["reference"],
            target=config["target"],
            period=config["period"],
            current_period=config["current_period"],
            revision_window=config["revision_window"],
            question_no=config["question_no"],
            strata="imputation_class",
            auxiliary=config["auxiliary_converted"],
            filters=filter_df,
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
        sic=config["sic"],
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
