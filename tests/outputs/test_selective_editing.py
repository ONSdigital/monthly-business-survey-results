from pathlib import Path
from unittest.mock import patch
from mbs_results.period_zero_se_wrapper import period_zero_se_wrapper
from mbs_results.final_outputs import run_final_outputs

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.selective_editing import (
    calculate_auxiliary_value,
    calculate_predicted_value,
    create_standardising_factor,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/selective_editing")


@pytest.fixture(scope="class")
def create_standardising_factor_data(filepath):
    return pd.read_csv(
        filepath / "create_standardising_factor_data.csv", index_col=False
    )


@pytest.fixture(scope="class")
def calculate_predicted_value_data(filepath):
    return pd.read_csv(filepath / "calculate_predicted_value_data.csv", index_col=False)


@pytest.fixture(scope="class")
def calculate_auxiliary_value_input(filepath):
    return pd.read_csv(
        filepath / "calculate_auxiliary_value_input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def calculate_auxiliary_value_output(filepath):
    return pd.read_csv(
        filepath / "calculate_auxiliary_value_output.csv", index_col=False
    )


class TestSelectiveEditing:
    def test_create_standardising_factor(
        self,
        create_standardising_factor_data,
    ):
        expected_output = create_standardising_factor_data[
            (create_standardising_factor_data["period"] == 202401)
            & (create_standardising_factor_data["question_code"].isin([40, 49]))
        ]
        expected_output = expected_output[
            [
                "period",
                "reference",
                "question_code",
                "standardising_factor",
                "predicted_value",
                "imputation_marker",
                "imputation_class",
            ]
        ]
        expected_output = expected_output.reset_index(drop=True)

        input_data = create_standardising_factor_data.drop(
            columns="standardising_factor"
        )

        actual_output = create_standardising_factor(
            input_data,
            "reference",
            "period",
            "domain",
            "question_code",
            "predicted_value",
            "imputation_marker",
            "imputation_class",
            "a_weight",
            "o_weight",
            "g_weight",
            period_selected=202401,
        )
        actual_output.drop(columns=["a_weight", "o_weight", "g_weight"], inplace=True)

        assert_frame_equal(actual_output, expected_output)

    def test_calculate_predicted_value(self, calculate_predicted_value_data):
        input_data = calculate_predicted_value_data.drop(columns="predicted_value")

        actual_output = calculate_predicted_value(
            input_data,
            "adjusted_value",
            "imputed_value",
        )

        assert_frame_equal(actual_output, calculate_predicted_value_data)

    def test_calculate_auxiliary_value(
        self, calculate_auxiliary_value_input, calculate_auxiliary_value_output
    ):

        input_data = calculate_auxiliary_value_input

        expected_output = calculate_auxiliary_value_output
        expected_output["auxiliary_value"] = expected_output["auxiliary_value"].astype(
            int
        )

        actual_output = calculate_auxiliary_value(
            input_data,
            "reference",
            "period",
            "question_no",
            "frozen_turnover",
            "construction_link",
            "imputation_class",
            202001,
        )
        actual_output.drop(
            columns=["construction_link", "frozen_turnover", "formtype"], inplace=True
        )

        assert_frame_equal(actual_output, expected_output)




@pytest.fixture(scope="class")
def mock_load_imputation_output():
    path = "tests/data/test_se_wrappers/output/imputation_test_snapshot.csv"
    with patch(
        "mbs_results.final_outputs.load_imputation_output",
        return_value=pd.read_csv(path)
    ) as mock_load:
        yield pd.read_csv(path)


class TestSelectiveEditingWrappers:
    def test_period_zero_se_wrapper(self, mock_user_config):
        period_zero_se_wrapper(config_user_dict=mock_user_config)

    def test_run_final_outputs(self, mock_user_config, mock_load_imputation_output):
        mock_user_config["current_period"] = 202201

        # Access the mock to ensure it's used and avoid the "not accessed" error
        assert mock_load_imputation_output is not None

        run_final_outputs(mock_user_config)
