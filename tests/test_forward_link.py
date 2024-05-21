import pytest
from helper_functions import load_and_format
from pandas.testing import assert_series_equal

from src.forward_link import calculate_imputation_link

scenarios = ["calculate_links_test_data"]


@pytest.mark.parametrize("scenario", scenarios)
class TestLinks:
    def test_forward_links(self, scenario):
        """Test if function returns the f_link column"""

        df_input = load_and_format("tests/" + scenario + ".csv")

        expected_link = df_input["f_link"]

        link_to_test = calculate_imputation_link(
            df_input,
            "period",
            "group",
            "f_matched_pair",
            "question",
            "f_predictive_question",
        )

        assert_series_equal(link_to_test, expected_link, check_names=False)

    def test_back_links(self, scenario):
        """Test if function returns the b_link column"""

        df_input = load_and_format("tests/" + scenario + ".csv")

        expected_link = df_input["b_link"]

        link_to_test = calculate_imputation_link(
            df_input,
            "period",
            "group",
            "b_matched_pair",
            "question",
            "b_predictive_question",
        )

        assert_series_equal(link_to_test, expected_link, check_names=False)
