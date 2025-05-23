import pandas as pd
import pytest

from mbs_results.outputs.get_additional_outputs import get_additional_outputs


def test1(**kwargs):
    print(1)  # dummy function


def test2(**kwargs):
    print(2)  # dummy function


def selective_editing_test1(**kwargs):
    print(1)  # dummy function


def selective_editing_test2(**kwargs):
    print(2)  # dummy function


@pytest.fixture(scope="class")
def function_mapper():
    return {"test1": test1, "test2": test2}


@pytest.fixture(scope="class")
def se_function_mapper():
    return {
        "selective_editing_test1": selective_editing_test1,
        "selective_editing_test2": selective_editing_test2,
    }


@pytest.mark.parametrize(
    "inp, expected, selective_editing",
    [
        ({"additional_outputs": ["all"]}, "1\n2\n", False),
        ({"additional_outputs": ["test1"]}, "1\n", False),
        ({"additional_outputs": ["test2"]}, "2\n", False),
        ({"additional_outputs": []}, "No additional_outputs produced\n", False),
        ({"additional_outputs": ["all"]}, "No additional_outputs produced\n", True),
        (
            {
                "additional_outputs": [
                    "test1",
                    "test2",
                    "selective_editing_test1",
                    "selective_editing_test2",
                ]
            },
            "1\n2\n",
            False,
        ),
    ],
)
def test_output(capsys, function_mapper, inp, expected, selective_editing):
    """Test that the right functions were run"""
    get_additional_outputs(inp, function_mapper, pd.DataFrame(), selective_editing)
    out, err = capsys.readouterr()
    assert out == expected


@pytest.mark.parametrize(
    "inp, expected, selective_editing",
    [
        ({"additional_outputs": ["all"]}, "1\n2\n", True),
        ({"additional_outputs": ["selective_editing_test1"]}, "1\n", True),
        ({"additional_outputs": ["selective_editing_test2"]}, "2\n", True),
        ({"additional_outputs": []}, "No additional_outputs produced\n", True),
        ({"additional_outputs": ["all"]}, "No additional_outputs produced\n", False),
        (
            {
                "additional_outputs": [
                    "test1",
                    "test2",
                    "selective_editing_test1",
                    "selective_editing_test2",
                ]
            },
            "1\n2\n",
            True,
        ),
    ],
)
def test_se_output(capsys, se_function_mapper, inp, expected, selective_editing):
    """Test that the right functions were run"""
    get_additional_outputs(inp, se_function_mapper, pd.DataFrame(), selective_editing)
    out, err = capsys.readouterr()
    assert out == expected


def test_raise_errors(function_mapper):
    """Test if error is raised when user doesn't pass a list or passes a
    function which does not link to a function"""

    with pytest.raises(ValueError):
        get_additional_outputs(
            {"additional_outputs": "not_a_list"}, function_mapper, pd.DataFrame()
        )

    with pytest.raises(ValueError):
        get_additional_outputs(
            {"additional_outputs": ["test3"]}, function_mapper, pd.DataFrame()
        )
