import logging
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results import configure_logger_with_run_id, logger
from mbs_results.utilities.constrains import (
    calculate_derived_outlier_weights,
    constrain,
    enforce_export_weight_constraint,
    replace_values_index_based,
    replace_with_manual_outlier_weights,
    sum_sub_df,
    update_derived_weight_and_winsorised_value,
)


@pytest.fixture(autouse=True)
def setup_test_logger():
    """Configure logger for testing to work with caplog."""
    # Store original state
    original_handlers = logger.handlers.copy()
    original_propagate = logger.propagate

    # Setup for testing
    logger.handlers.clear()
    logger.propagate = True

    # Configure with test settings
    test_config = {"run_id": "test-run", "platform": "network", "output_path": None}
    configure_logger_with_run_id(test_config)

    yield

    # Restore original state
    logger.handlers.clear()
    logger.handlers.extend(original_handlers)
    logger.propagate = original_propagate


@pytest.fixture(scope="class")
def filepath(utilities_data_dir):
    return utilities_data_dir / "constrains"


def test_replace_values_index_base(filepath):

    df = pd.read_csv(filepath / "test_replace_values_index_based.csv", index_col=False)

    df = df.set_index(["question_no", "period", "reference"])

    df_in = df[["target"]].copy()

    df_expected = df[["expected", "constrain_marker"]].rename(
        columns={"expected": "target"}
    )
    df_expected.sort_index(inplace=True)

    replace_values_index_based(df_in, "target", 49, ">", 40)
    replace_values_index_based(df_in, "target", 90, ">=", 40)

    # Enforce dtypes, otherwise null==null fails
    df_in["constrain_marker"] = df_in["constrain_marker"].astype(str)
    df_expected["constrain_marker"] = df_expected["constrain_marker"].astype(str)

    assert_frame_equal(df_in, df_expected)


def test_sum_sub_df_46_47(filepath):

    df = pd.read_csv(filepath / "test_sum_sub_df.csv", index_col=False)

    df_in = (
        df.loc[df["question_no"] != 40]
        .drop(columns=["constrain_marker"])
        .set_index(["question_no", "period", "reference"])
    )

    expected_output = (
        df.loc[df["question_no"] == 40]
        .drop(columns=["question_no"])
        .reset_index(drop=True)
    )

    actual_ouput = (
        sum_sub_df(df_in, [46, 47]).sort_values(by="reference").reset_index(drop=True)
    )

    assert_frame_equal(actual_ouput, expected_output)


def test_constrain_functionality(filepath):
    df = pd.read_csv(
        filepath / "test_constrain.csv",
        index_col=False,
    )
    # Creating dummy columns needed for constrains, not used other than setting as index
    for col_name in ["cell_no", "converted_frotover", "froempment", "frosic2007"]:
        df[col_name] = 1

    df["target"] = df["target"].astype(float)
    df["spp_form_id"] = df["spp_form_id"].astype(int)

    # Drop q40 rows for form 13 and 14 and q46 for form 15
    df_input = (
        df.drop(
            df[(df["question_no"] == 40) & (df["spp_form_id"].isin([13, 14]))].index
        )
        .drop(df[(df["question_no"] == 46) & (df["spp_form_id"].isin([15]))].index)
        .drop(df[(df["question_no"] == 47) & (df["spp_form_id"].isin([15]))].index)
        .drop(df[(df["question_no"] == 42) & (df["spp_form_id"].isin([16]))].index)
        .drop(df[(df["question_no"] == 43) & (df["spp_form_id"].isin([16]))].index)
        .drop(
            columns=[
                "pre_derived_target",
                "expected_target",
                "pre_constrained_target",
                "constrain_marker",
            ]
        )
    )

    df_expected_output = df.drop(
        columns=["cell_no", "converted_frotover", "froempment", "frosic2007", "target"]
    ).rename(columns={"expected_target": "target"})
    df_expected_output["target"] = df_expected_output["target"].astype(float)

    df_output = constrain(
        df_input,
        "period",
        "reference",
        "target",
        "question_no",
        "spp_form_id",
        "frosic2007",
    )

    # Dropping dummy columns as these are unchanged in function
    order = [
        "period",
        "reference",
        "spp_form_id",
        "question_no",
        "target",
        "pre_derived_target",
        "constrain_marker",
        "pre_constrained_target",
    ]

    df_output.drop(
        columns=["cell_no", "converted_frotover", "froempment", "frosic2007"],
        inplace=True,
    )
    df_output = df_output[order].sort_values(by=order).reset_index(drop=True)

    df_expected_output = (
        df_expected_output[order].sort_values(by=order).reset_index(drop=True)
    )
    df_expected_output["spp_form_id"] = df_expected_output["spp_form_id"].astype(
        "int64"
    )

    assert_frame_equal(df_output, df_expected_output)


class TestDerivedOutlierWeights:
    def test_calculate_derived_outlier_weights(self, filepath):
        config = {"sic": "frosic2007"}
        df = pd.read_csv(
            filepath / "derived-questions-winsor.csv",
            index_col=False,
        )
        df["target_variable"] = df["target_variable"].astype(float)
        df["new_target_variable"] = df["new_target_variable"].astype(float)
        # Drop q40 rows
        df_input = df.drop(df[df["question_no"] == 40].index)
        df_input.drop(
            columns=["default_o_weight", "constrain_marker", "post_wins_marker"],
            inplace=True,
        )

        df_output = calculate_derived_outlier_weights(
            df_input,
            "period",
            "reference",
            "target_variable",
            "question_no",
            "spp_form_id",
            "outlier_weight",
            "new_target_variable",
            config,
        )

        sorting_by = ["reference", "period", "question_no", "spp_form_id"]
        input_col_order = df.columns
        df_output = (
            df_output[input_col_order].sort_values(by=sorting_by).reset_index(drop=True)
        )
        df = df.sort_values(by=sorting_by).reset_index(drop=True)

        assert_frame_equal(df, df_output)

    def test_calculate_derived_outlier_weights_missing(self, filepath):
        config = {"sic": "frosic2007"}
        df = pd.read_csv(
            filepath / "derived-questions-winsor-missing.csv",
            index_col=False,
        )
        df["target_variable"] = df["target_variable"].astype(float)
        df["new_target_variable"] = df["new_target_variable"].astype(float)
        # Drop q40 rows
        df_input = df.drop(df[df["question_no"] == 40].index)
        df_input.drop(
            columns=["default_o_weight", "constrain_marker", "post_wins_marker"],
            inplace=True,
        )
        # Manually change the input data to be missing one value in
        # new_target_variable . data is present in dataset to compare against
        df_input.loc[
            (df_input["reference"] == 101)
            & (df_input["period"] == 202401)
            & (df_input["question_no"] == 46)
            & (df_input["spp_form_id"] == 13),
            "new_target_variable",
        ] = None

        df_output = calculate_derived_outlier_weights(
            df_input,
            "period",
            "reference",
            "target_variable",
            "question_no",
            "spp_form_id",
            "outlier_weight",
            "new_target_variable",
            config,
        )

        sorting_by = ["reference", "period", "question_no", "spp_form_id"]
        input_col_order = df.columns
        df_output = (
            df_output[input_col_order].sort_values(by=sorting_by).reset_index(drop=True)
        )
        df = df.sort_values(by=sorting_by).reset_index(drop=True)

        assert_frame_equal(df, df_output)

    scenarios = [
        "no_further_processing_required",
        "outlier_identified_example_1",
        "outlier_identified_example_2",
        "outlier_identified_example_q46_q47",
        "outlier_identified_example_derived_q42",
        "outlier_identified_example_derived_q46",
        "outlier_identifies_non_contributor",
        "outlier_identifies_example_3",
    ]

    @pytest.mark.parametrize("base_file_name", scenarios)
    def test_update_derived_weight_and_winsorised_value(self, filepath, base_file_name):

        df_in = pd.read_csv(filepath / Path(base_file_name + ".csv"))
        df_expected = pd.read_csv(filepath / Path(base_file_name + "_expected.csv"))
        df_actual = update_derived_weight_and_winsorised_value(
            df_in,
            "reference",
            "period",
            "questioncode",
            "spp_form_id",
            "outlier_weight",
            "value",
        )

        assert_frame_equal(df_actual, df_expected)

    def test_enforce_export_weight_constraint(self, filepath):
        df_in = pd.read_csv(filepath / "test_enforce_export_weight_in.csv")
        df_expected = pd.read_csv(filepath / "test_enforce_export_weight_out.csv")

        df_actual = enforce_export_weight_constraint(
            df_in,
            reference="reference",
            period="period",
            question_code="questioncode",
            outlier_weight="outlier_weight",
            target="value",
        )

        assert_frame_equal(df_actual, df_expected)

    def test_replace_outlier_weights(self, filepath):

        df = pd.read_csv(filepath / "test_replace_outliers_in.csv", index_col=False)

        df_in = df.drop(columns=["manual_outlier_weight"])

        df_expected = pd.read_csv(
            filepath / "test_replace_outliers_out.csv", index_col=False
        )

        test_config = {
            "manual_outlier_path": filepath / "manual_outliers.csv",
            "platform": "network",
            "bucket": "",
        }

        df_actual = replace_with_manual_outlier_weights(
            df_in,
            "reference",
            "period",
            "question_no",
            "outlier_weight",
            test_config,
        )

        assert_frame_equal(df_actual, df_expected)


class TestManualOutliers:
    def test_no_manual_outliers(self, filepath):

        df = pd.read_csv(filepath / "test_replace_outliers_in.csv", index_col=False)

        df_in = df.drop(columns=["manual_outlier_weight"])

        test_config = {"manual_outlier_path": "", "platform": "network", "bucket": ""}

        df_actual = replace_with_manual_outlier_weights(
            df_in,
            "reference",
            "period",
            "question_no",
            "outlier_weight",
            test_config,
        )

        assert_frame_equal(df_actual, df_in)

    def test_manual_outliers_unmatched_warning(self, filepath, caplog):

        df = pd.read_csv(filepath / "test_replace_outliers_in.csv", index_col=False)

        df_in = df.drop(columns=["manual_outlier_weight"])

        test_config = {
            "manual_outlier_path": filepath / "manual_outliers.csv",
            "platform": "network",
            "bucket": "",
        }

        # Set log level without context manager
        caplog.set_level(logging.WARNING)
        replace_with_manual_outlier_weights(
            df_in,
            "reference",
            "period",
            "question_no",
            "outlier_weight",
            test_config,
        )

        # Check log records directly
        assert any(
            "There are 1 unmatched references" in record.message
            for record in caplog.records
        )
