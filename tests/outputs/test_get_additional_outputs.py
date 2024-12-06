import pytest
import pandas as pd

from mbs_results.outputs.get_additional_outputs import get_additional_outputs


def test1(**kwargs):
    print(1)  # dummy function


def test2(**kwargs):
    print(2)  # dummy function


@pytest.fixture(scope="class")
def function_mapper():
    return {"test1": test1, "test2": test2}


@pytest.mark.parametrize(
    "inp, expected",
    [
        ({"additional_outputs": ["all"]}, "1\n2\n"),
        ({"additional_outputs": ["test1"]}, "1\n"),
        ({"additional_outputs": ["test2"]}, "2\n"),
        ({"additional_outputs": []}, "No additional_outputs produced\n"),
    ],
)
def test_output(capsys, function_mapper, inp, expected):
    """Test that the right functions were run"""
    get_additional_outputs(inp, function_mapper, pd.DataFrame())
    out, err = capsys.readouterr()
    assert out == expected


def test_raise_errors(function_mapper):
    """Test if error is raised when user doesn't pass a list or passes a
    function which does not link to a function"""

    with pytest.raises(ValueError):
        get_additional_outputs({"additional_outputs": "not_a_list"}, function_mapper, pd.DataFrame())

    with pytest.raises(ValueError):
        get_additional_outputs({"additional_outputs": ["test3"]}, function_mapper, pd.DataFrame())
