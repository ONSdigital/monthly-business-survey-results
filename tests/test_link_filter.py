import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.link_filter import flag_rows_to_ignore


@pytest.mark.parametrize("scenario", ["test_flag_data"])
@pytest.mark.parametrize("filters", ["test_flag_filters"])
class TestFilters:
    def test_basic_filter(self, scenario, filters):
        """Test ignore_from_link is correct"""

        df_output_expected = pd.read_csv("tests/" + scenario + ".csv")

        df_filters = pd.read_csv("tests/" + filters + ".csv")

        df_input = df_output_expected.drop(columns=["ignore_from_link"])

        df_output = flag_rows_to_ignore(df_input, df_filters)

        assert_frame_equal(df_output, df_output_expected)

    def test_exception(self, scenario, filters):

        """Test if function raises an exception when the columns in filters
        do not exist in scenario."""

        df_output_expected = pd.read_csv("tests/" + scenario + ".csv")

        df_filters = pd.read_csv("tests/" + filters + ".csv")

        df_input = df_output_expected.drop(columns=["ignore_from_link"])

        with pytest.raises(ValueError):

            df_filters.columns = df_filters.columns + "_fail"

            flag_rows_to_ignore(df_input, df_filters)
