import pytest
import pandas as pd
import numpy as np

from pandas.testing import assert_frame_equal

from src.construction_matches import flag_construction_matches

def test_construction_matches():
    expected_output = pd.DataFrame(np.array([
            [42,pd.to_datetime("202401", format="%Y%m"),10,True],
            [pd.NA,pd.to_datetime("202401", format="%Y%m"),10,False],
        ]),
        columns=["target", "period", "auxiliary", "flag_construction_matches"],
    )

    # cast to python non-nullable bool type rather than pandas nullable boolean type
    expected_output["flag_construction_matches"] = expected_output["flag_construction_matches"].astype(bool)

    input_data = expected_output.drop(labels=["flag_construction_matches"], axis=1)
    actual_output = flag_construction_matches(input_data, "target", "period", "auxiliary")

    assert_frame_equal(actual_output, expected_output)