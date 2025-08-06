from unittest.mock import patch

import pandas as pd

from mbs_results.staging.stage_dataframe import check_construction_links


@patch("pandas.DataFrame.to_csv")
@patch("mbs_results.staging.stage_dataframe.logger")
class TestCheckConstructionLinks:
    def test_check_construction_links(self, mock_logger, mock_to_csv):
        df_input = pd.DataFrame(
            {
                "construction_link": [0.1, 0.2, 0.5, 1, 2, 2],
                "question_no": [49, 49, 49, 49, 49, 40],
                "reference": [101, 102, 103, 104, 105, 106],
            }
        )
        test_config = {
            "question_no": "question_no",
            "reference": "reference",
            "output_path": "",
            "current_period": 201901,
        }
        check_construction_links(df_input, test_config)

        mock_logger.info.assert_any_call(
            "checking values for construction link for q49"
        )
        mock_logger.warning.assert_any_call(
            "number of records with construction link > 1 for q49: 1"
        )
        mock_logger.info.assert_any_call(
            "references with construction link > 1 for "
            + "q49 saved to q49_references_con_link_greater_1_201901.csv"
        )

    def test_check_construction_links_no_warning(self, mock_logger, mock_to_csv):
        df_input = pd.DataFrame(
            {
                "construction_link": [0.1, 0.2, 0.5, 1, 0.5, 2],
                "question_no": [49, 49, 49, 49, 49, 40],
                "reference": [101, 102, 103, 104, 105, 106],
            }
        )
        test_config = {
            "question_no": "question_no",
            "reference": "reference",
            "output_path": "",
            "current_period": 201901,
        }
        check_construction_links(df_input, test_config)

        mock_logger.info.assert_any_call(
            "checking values for construction link for q49"
        )
