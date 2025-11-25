import pandas as pd
import pytest
from unittest import TestCase
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp

from mbs_results.outputs.new_back_data import get_backdata_from_period



@pytest.fixture(scope="class")
def test_config():
    return {

    "imputation_marker_col":"imputation_flags_adjustedresponse",
    "status": "statusencoded",
    "period": "period",
    "question_no": "questioncode",
    "reference": "reference",
    "strata": "cell_no",
    "target": "adjustedresponse",
    "nil_status_col": "status",
    "run_id":"test1"
    }

@pytest.fixture(scope="class")
def new_back_data_input(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "new_back_data" / "new_back_data_input.csv",
        index_col=False,
    )

@pytest.fixture(scope="class")
def new_back_data_output(outputs_data_dir):
    contributors, responses = get_dfs_from_spp(

        outputs_data_dir / "new_back_data" / "new_back_data_output.json",
        "network",
        "")
    
    contributors = contributors.to_dict('list')
    responses = responses.to_dict('list')

    return contributors, responses


class TestNewBackData:
    def test_new_backdata_responses(
        self,
        new_back_data_input,
        new_back_data_output,
        test_config
    ):

        actual_output = get_backdata_from_period(
            new_back_data_input,
            202201,
            test_config,
        )

        _, responses_expected = new_back_data_output

        responses_actual  = actual_output["responses"]

        TestCase().assertDictEqual(responses_actual, responses_expected)
        
   
    def test_new_backdata_contributors(
            self,
            new_back_data_input,
            new_back_data_output,
            test_config
        ):

            actual_output = get_backdata_from_period(
                new_back_data_input,
                202201,
                test_config,
            )

            contributors_expected, _ = new_back_data_output

            contributors_actual =  actual_output["contributors"]
            
            TestCase().assertDictEqual(contributors_actual, contributors_expected)

