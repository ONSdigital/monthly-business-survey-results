import logging

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import produce_additional_outputs
from mbs_results.staging.back_data import read_and_process_back_data
from mbs_results.staging.data_cleaning import create_imputation_class
from mbs_results.staging.stage_dataframe import drop_derived_questions
from mbs_results.utilities.constrains import constrain
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import qa_selective_editing_outputs

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="test.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def back_data_wrapper():

    config = load_config(path="./mbs_results/config.json")

    # Read in back data
    back_data = read_and_process_back_data(config)

    # back_data = enfoce_dtypes...()

    # Lots of "glueing" other functions together so df is in a format for estimation
    # Another method could be to look at imputation, and try to keep original imputation
    # markers.

    back_data = drop_derived_questions(
        back_data,
        config["question_no"],
        config["form_id_spp"],
    )

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
    back_data_imputation = calculate_imputation_link(
        back_data_cons_matches,
        match_col="flag_construction_matches",
        link_col="construction_link",
        predictive_variable=config["auxiliary"],
        **config,
    )

    # Changing period back into int. Read_colon_sep_file should be updated to enforce
    # data types as per the config.

    back_data_imputation["period"] = (
        back_data_imputation["period"].dt.strftime("%Y%m").astype("int")
    )
    back_data_imputation["frosic2007"] = back_data_imputation["frosic2007"].astype(
        "str"
    )

    # Running all of estimation and outliers
    back_data_estimation = estimate(back_data_imputation, config)

    back_data_outliering = detect_outlier(back_data_estimation, config)

    additional_outputs_df = back_data_estimation[
        [
            "reference",
            "period",
            "design_weight",
            "frosic2007",
            "formtype",
            "questioncode",
            "frotover",
            "calibration_factor",
            "adjustedresponse",
            "response",
            "froempment",
            "cell_no",
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

    return back_data_outliering


if __name__ == "__main__":
    # config = load_config(path="./mbs_results/config.json")
    # qa_selective_editing_outputs(config)
    print("wrapper start")
    back_data_wrapper()
    print("wrapper end")
