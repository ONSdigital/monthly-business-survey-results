from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.stage_dataframe import start_of_period_staging


@pytest.fixture(scope="class")
def filepath():
    return Path(r"tests\data\outputs\selective_editing")


@pytest.fixture(scope="class")
def imputation_output(filepath):
    return pd.read_csv(
        filepath / "inputs/imputation_output_v0.1.1_test_snaphot.csv", index_col=False
    )


@pytest.fixture(scope="class")
def start_of_period_staging_output(filepath):
    return pd.read_csv(filepath / "start_of_period_staging_output.csv", index_col=False)


input_path = r"tests/data/test_main/input/"

config = {
    "period_selected": 202202,
    "current_period": 202201,
    "finalsel_keep_cols": [
        "formtype",
        "cell_no",
        "froempment",
        "frosic2007",
        "frotover",
        "period",
        "reference",
    ],
    "sample_path": input_path + "test_finalsel009_*",
    "sample_column_names": [
        "reference",
        "checkletter",
        "frosic2003",
        "rusic2003",
        "frosic2007",
        "rusic2007",
        "froempees",
        "employees",
        "froempment",
        "employment",
        "froFTEempt",
        "FTEempt",
        "frotover",
        "turnover",
        "entref",
        "wowentref",
        "vatref",
        "payeref",
        "crn",
        "live_lu",
        "live_vat",
        "live_paye",
        "legalstatus",
        "entrepmkr",
        "region",
        "birthdate",
        "entname1",
        "entname2",
        "entname3",
        "runame1",
        "runame2",
        "runame3",
        "ruaddr1",
        "ruaddr2",
        "ruaddr3",
        "ruaddr4",
        "ruaddr5",
        "rupostcode",
        "tradstyle1",
        "tradstyle2",
        "tradstyle3",
        "contact",
        "telephone",
        "fax",
        "seltype",
        "inclexcl",
        "cell_no",
        "formtype",
        "cso_tel",
        "currency",
    ],
    "idbr_to_spp": {
        "201": 9,
        "202": 9,
        "203": 10,
        "204": 10,
        "205": 11,
        "216": 11,
        "106": 12,
        "111": 12,
        "117": 13,
        "167": 13,
        "123": 14,
        "173": 14,
        "817": 15,
        "867": 15,
        "823": 16,
        "873": 16,
    },
    "form_id_spp": "form_type_spp",
    "form_id_idbr": "formtype",
    "reference": "reference",
    "period": "period",
    "question_no": "questioncode",
    "master_column_type_dict": {
        "reference": "int",
        "period": "date",
        "response": "str",
        "questioncode": "int",
        "adjustedresponse": "float",
        "frozensic": "str",
        "frozenemployees": "int",
        "frozenturnover": "float",
        "cellnumber": "int",
        "formtype": "str",
        "status": "str",
        "statusencoded": "int",
        "frosic2007": "str",
        "froempment": "int",
        "frotover": "float",
        "cell_no": "int",
    },
    "temporarily_remove_cols": [],
}


class TestStartOfPeriodStaging:
    def test_start_of_period_staging(
        self, imputation_output, start_of_period_staging_output
    ):
        expected_output = start_of_period_staging_output
        expected_output.to_csv("./outputs/expected_output.csv", index=False)

        actual_output = start_of_period_staging(imputation_output, config)
        actual_output.to_csv("./outputs/actual_output.csv", index=False)

        assert_frame_equal(actual_output, expected_output)
