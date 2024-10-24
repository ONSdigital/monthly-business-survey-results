import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.calculate_imputation_link import calculate_imputation_link
from tests.helper_functions import load_and_format


@pytest.fixture(scope="class")
def forward_backward_test_data():
    return load_and_format(
        "tests/data/imputation/calculate_imputation_link/forward_backward.csv"
    )


@pytest.fixture(scope="class")
def construction_test_data():
    return load_and_format(
        "tests/data/imputation/calculate_imputation_link/construction.csv"
    )


class TestLinks:
    @pytest.mark.parametrize(
        "test_data,match,predictive,link",
        [
            (
                "forward_backward_test_data",
                "f_matched_pair",
                "f_predictive_question",
                "f_link",
            ),
            (
                "forward_backward_test_data",
                "b_matched_pair",
                "b_predictive_question",
                "b_link",
            ),
            (
                "construction_test_data",
                "flag_construction_matches",
                "auxiliary",
                "construction_link",
            ),
        ],
    )
    def test_calculate_imputation_links(
        self, test_data, match, predictive, link, request
    ):
        """Test if function returns the f_link column"""
        df_output = request.getfixturevalue(test_data)

        df_input = df_output.drop(columns=[link])

        df_input = calculate_imputation_link(
            df_input,
            "period",
            "group",
            match,
            "target",
            predictive,
            link,
        )
        df_input.drop(
            columns=[match + "_pair_count", "default_link_" + match], inplace=True
        )

        assert_frame_equal(df_input, df_output, check_like=True)
