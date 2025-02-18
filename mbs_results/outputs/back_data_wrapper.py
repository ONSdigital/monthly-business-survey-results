import logging

from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from mbs_results.imputation.construction_matches import flag_construction_matches
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import produce_additional_outputs
from mbs_results.staging.back_data import read_and_process_back_data
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

    config = load_config(path="./produce_se_outputs/config.json")

    # Read in back data
    back_data = read_and_process_back_data(config)
    # back_data = enfoce_dtypes...()

    # Run apply_imputation_link function to get construction links
    back_data_cons_matches = flag_construction_matches(back_data, **config)
    back_data_imputation = calculate_imputation_link(
        back_data_cons_matches,
        match_col="flag_construction_matches",
        link_col="construction_link",
        predictive_variable=config["auxiliary"],
        **config,
    )

    # Changing period back into int. Read_colon_sep_file should be updated to enforce data
    # types as per the config.

    back_data_imputation["period"] = (
        back_data_imputation["period"].dt.strftime("%Y%m").astype("int")
    )
    back_data_imputation["frosic2007"] = back_data_imputation["frosic2007"].astype(
        "str"
    )

    # Running all of estimation and outliers
    back_data_estimation = estimate(back_data_imputation, config)
    back_data_outliering = detect_outlier(back_data_estimation, config)

    back_data_output = back_data_outliering[
        [
            "period",
            "reference",
            "questioncode",
            "design_weight",
            "frosic2007",
            "formtype",
            "adjusted_value",
            "design_weight",
            "outlier_weight",
            "calibration_factor",
            "frotover",
            "construction_link",
            "response_type",  # In place of imputation marker - is this correct?
        ]
    ]

    # Link to produce_additional_outputs
    produce_additional_outputs(config, back_data_output)

    qa_selective_editing_outputs(config)

    return back_data_outliering


if __name__ == "__main__":
    print("wrapper start")
    back_data_wrapper()
    print("wrapper end")
