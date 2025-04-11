import numpy as np
import pandas as pd
import pytest
import logging
import tempfile

from mbs_results.utilities.validation_checks import (
    colnames_clash,
    period_and_reference_not_given,
    validate_config_datatype_input,
    validate_config_repeated_datatypes,
    validate_estimation,
    validate_indices,
    validate_outlier_detection
)


def test_colnames_clash():
    test_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {
            "period": "date",
            "reference": "int",
            "strata": "str",
        },
        "contributors_keep_cols": {
            "reference": "int",
            "period": "date",
            "strata": "str",
        },
        "temporarily_remove_cols": [],
    }
    assert colnames_clash(**test_config) is True


def test_period_and_reference_not_given():
    test_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {
            "reference": "int",
            "strata": "str",
        },
        "contributors_keep_cols": {
            "reference": "int",
            "period": "date",
            "strata": "str",
        },
        "temporarily_remove_cols": [],
    }
    assert period_and_reference_not_given(**test_config) is True


def test_validate_indices():
    dictionary_data = {
        "reference": ["1"],
        "period": ["202212"],
        "target_variable": ["20"],
        "strata": ["101"],
    }
    responses = pd.DataFrame(data=dictionary_data).set_index(["reference", "period"])
    dictionary_data["reference"] = ["2"]
    contributors = pd.DataFrame(data=dictionary_data).set_index(["reference", "period"])
    with pytest.raises(ValueError):
        validate_indices(responses, contributors)


def test_validate_config_datatype_input():
    test_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {
            "period": "date",
            "reference": "int",
        },
        "contributors_keep_cols": {
            "reference": "int",
            "period": "date",
            "strata": "string",
        },
        "temporarily_remove_cols": [],
    }
    with pytest.raises(ValueError):
        validate_config_datatype_input(**test_config)


def test_validate_config_repeated_datatypes():
    test_config = {
        "period": "period",
        "reference": "reference",
        "responses_keep_cols": {
            "period": "date",
            "reference": "int",
        },
        "contributors_keep_cols": {
            "reference": "int",
            "period": "str",
            "strata": "str",
        },
        "temporarily_remove_cols": [],
    }
    with pytest.raises(ValueError):
        validate_config_repeated_datatypes(**test_config)


class TestValidateEstimation:

    test_config = {
        "census": "is_census",
        "sampled": "is_sampled",
        "mbs_file_name": "test_snaphot.json",
        "output_path": "tests/data/utilities/validation_checks/outputs/",
    }

    def test_validate_estimation_null_census(self):
        test_data_dict = {
            "is_sampled": [True, False, False, True, False],
            "is_census": [False, True, True, np.nan, False],
        }

        test_data = pd.DataFrame(data=test_data_dict)

        with pytest.raises(ValueError):
            validate_estimation(df=test_data, config=self.test_config)

    def test_validate_estimation_null_sampled(self):
        test_data_dict = {
            "is_sampled": [True, False, False, np.nan, False],
            "is_census": [False, True, True, False, False],
        }

        test_data = pd.DataFrame(data=test_data_dict)

        with pytest.raises(ValueError):
            validate_estimation(df=test_data, config=self.test_config)

def test_validate_outlier_weight_error(caplog):
    # Create a temporary directory for the output path
    with tempfile.TemporaryDirectory() as temp_dir:
        test_config = {
            "design_weight": "design_weight",
            "output_path": temp_dir,  # Use the temporary directory
            "mbs_file_name": "test_snapshot.json"
        }
        
        test_data_dict = {
            "reference": [1, 2, 3, 4],
            "design_weight": [1, 1, 0.5, 0.3],
            "outlier_weight": [0.3, 1, 0.6, 0.8]
        }

        test_data = pd.DataFrame(data=test_data_dict)

        with caplog.at_level(logging.ERROR):
            validate_outlier_detection(df=test_data, config=test_config)

        # Assert that the error message is captured in the logs
        assert "There are instances where the design weight = 1 and outlier_weight != 1." in caplog.text