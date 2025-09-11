from mbs_results.estimation.estimate import estimate
from mbs_results.imputation.impute import impute
from mbs_results.outlier_detection.detect_outlier import detect_outlier
from mbs_results.outputs.produce_additional_outputs import (
    get_additional_outputs_df,
    produce_additional_outputs,
)
from mbs_results.staging.stage_dataframe import stage_dataframe
from mbs_results.utilities.inputs import load_config
from mbs_results.utilities.validation_checks import (
    validate_config,
    validate_estimation,
    validate_imputation,
    validate_outlier_detection,
    validate_staging,
)


def run_mbs_main(config_user_dict=None):
    config = load_config("config_user.json",config_user_dict)
    validate_config(config)

    df, manual_constructions, filter_df = stage_dataframe(config)
    validate_staging(df, config)

    # imputation: RoM wrapper -> Rename wrapper to apply_imputation
    df = impute(df, manual_constructions, config, filter_df)
    validate_imputation(df, config)

    # Estimation Wrapper
    df = estimate(
        df=df, method="combined", convert_NI_GB_cells=True, config=config
    )
    validate_estimation(df, config)

    # Outlier Wrapper
    df = detect_outlier(df, config)
    validate_outlier_detection(df, config)

    df = get_additional_outputs_df(df, config
    )
    produce_additional_outputs(
        additional_outputs_df = df,
        QA_outputs = True,
        additional_outputs = False,
        config = config)

if __name__ == "__main__":
    run_mbs_main()
