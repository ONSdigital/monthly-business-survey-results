import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from src.forward_link import calculate_imputation_link

scenarios = ["calculate_links_test_data"]


@pytest.mark.parametrize("scenario", scenarios)
class TestLinks:
    def test_forward_links(self, scenario):
        """Test if function returns the f_link column"""

        df_output = load_and_format("tests/" + scenario + ".csv")

        df_input = df_output.drop(columns=["f_link"])

        df_input = calculate_imputation_link(
            df_input,
            "period",
            "group",
            "f_matched_pair",
            "question",
            "f_predictive_question",
        )

        assert_frame_equal(df_input, df_output, check_like=True)

    def test_back_links(self, scenario):
        """Test if function returns the b_link column"""
        df_output = load_and_format("tests/" + scenario + ".csv")

        df_input = df_output.drop(columns=["b_link"])

        df_input = calculate_imputation_link(
            df_input,
            "period",
            "group",
            "b_matched_pair",
            "question",
            "b_predictive_question",
        )

        assert_frame_equal(df_input, df_output, check_like=True)

    def test_exception(self, scenario):

        df = load_and_format("tests/" + scenario + ".csv")

        with pytest.raises(ValueError):
            """
            Test if function is called with wrong arguments, in particular
            with f_matched_pair and b_predictive_question or with
            b_matched_pair and f_predictive_question.
            """

            df = calculate_imputation_link(
                df,
                "period",
                "group",
                "f_matched_pair",
                "question",
                "b_predictive_question",
            )
        with pytest.raises(ValueError):

            df = calculate_imputation_link(
                df,
                "period",
                "group",
                "b_matched_pair",
                "question",
                "f_predictive_question",
            )
