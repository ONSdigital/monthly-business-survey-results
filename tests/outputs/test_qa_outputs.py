import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.qa_output import produce_qa_output


@pytest.fixture(scope="class")
def input_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "qa_output" / "qa_output_input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def output_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "qa_output" / "qa_output_output.csv", index_col=False
    )


@pytest.fixture(scope="class")
def test_config():
    return {
        "auxiliary": "frotover",
        "auxiliary_converted": "converted_frotover",
        "calibration_factor": "calibration_factor",
        "cell_number": "cell_no",
        "design_weight": "design_weight",
        "status": "statusencoded",
        "form_id_idbr": "formtype",
        "sic": "frosic2007",
        "group": "calibration_group",
        "calibration_group": "calibration_group",
        "period": "period",
        "question_no": "questioncode",
        "reference": "reference",
        "region": "region",
        "sampled": "is_sampled",
        "census": "is_census",
        "strata": "cell_no",
        "target": "adjustedresponse",
        "form_id_spp": "form_type_spp",
        "l_value_question_no": "question_no",
        "filter": None,
        "mandatory_outputs": ["produce_qa_output"],
        "filter_out_questions": [11, 12, 146],
        "question_no_plaintext": {
            "11": "start_date",
            "12": "end_date",
            "146": "comments",
        },
        "pound_thousand_col": "adjustedresponse_pounds_thousands",
    }


class TestProduceQaOutput:
    def test_qa_output_no_filter(
        self,
        test_config,
        input_df,
        output_df,
    ):
        expected_output = output_df

        actual_output = produce_qa_output(
            additional_outputs_df=input_df,
            **test_config,
        )

        assert_frame_equal(
            actual_output.sort_index(axis=1),
            expected_output.sort_index(axis=1),
        )

    def test_qa_output_filter(
        self,
        test_config,
        input_df,
        output_df,
    ):
        # changing input/output to match names when filters are applied
        test_config["filter"] = "filter.csv"

        rename_columns = {
            "b_match_adjustedresponse_count": "b_match_filtered_adjustedresponse_count",
            "f_match_adjustedresponse_count": "f_match_filtered_adjustedresponse_count",
        }

        input_df.rename(columns=rename_columns, inplace=True)
        output_df.rename(columns=rename_columns, inplace=True)

        expected_output = output_df

        actual_output = produce_qa_output(
            additional_outputs_df=input_df,
            **test_config,
        )

        assert_frame_equal(
            actual_output.sort_index(axis=1),
            expected_output.sort_index(axis=1),
        )
