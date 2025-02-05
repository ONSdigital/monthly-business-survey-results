from unittest.mock import MagicMock

import pandas as pd

from mbs_results.utilities.csw_to_spp_converter import validate_nil_markers


def test_validate_nil_markers():

    mock_logger = MagicMock()

    cp_df = pd.DataFrame(
        {
            "period": [202101, 202102, 202103],
            "reference": [1, 2, 3],
            "form_type": [1, 2, 3],
            "sic92": [4, 5, 6],
            "error_mkr": [7, 8, 9],
            "response_type": [3, 4, 5],
        }
    )

    qv_df = pd.DataFrame(
        {
            "period": [202101, 202102, 202103],
            "reference": [1, 2, 3],
            "question_number": [1, 2, 3],
            "adjusted_value": [10, 20, 30],
        }
    )

    result_df = validate_nil_markers(cp_df, qv_df, mock_logger)

    expected_df = pd.DataFrame(
        {
            "period": [202101, 202102, 202103],
            "reference": [1, 2, 3],
            "question_number": [1, 2, 3],
            "adjusted_value": [10, 0, 0],
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df)
