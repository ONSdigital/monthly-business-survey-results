import pandas as pd
import pytest

from mbs_results.outputs.get_additional_outputs import get_additional_outputs


def dummy_test1(**kwargs):
    print(1)  # dummy function


def dummy_test2(**kwargs):
    print(2)  # dummy function


def selective_editing_test1(**kwargs):
    print(1)  # dummy function


def selective_editing_test2(**kwargs):
    print(2)  # dummy function


@pytest.fixture(scope="class")
def function_mapper():
    return {"test1": dummy_test1, "test2": dummy_test2}


@pytest.fixture(scope="class")
def se_function_mapper():
    return {
        "selective_editing_test1": selective_editing_test1,
        "selective_editing_test2": selective_editing_test2,
    }


@pytest.mark.parametrize(
    "inp, expected, selective_editing",
    [
        # testing optional outputs and no mandatory outputs
        ({"optional_outputs": ["all"], "mandatory_outputs": []}, "1\n2\n", False),
        ({"optional_outputs": ["test1"], "mandatory_outputs": []}, "1\n", False),
        ({"optional_outputs": ["test2"], "mandatory_outputs": []}, "2\n", False),
        (
            {"optional_outputs": [], "mandatory_outputs": []},
            "No additional_outputs produced\n",
            False,
        ),
        (
            {"optional_outputs": ["all"], "mandatory_outputs": []},
            "No additional_outputs produced\n",
            True,
        ),
        (
            {
                "optional_outputs": [
                    "test1",
                    "test2",
                    "selective_editing_test1",
                    "selective_editing_test2",
                ],
                "mandatory_outputs": [],
            },
            "1\n2\n",
            False,
        ),
        # testing mandatory outputs and no optional outputs
        (
            {"optional_outputs": [], "mandatory_outputs": ["test1", "test2"]},
            "1\n2\n",
            False,
        ),
        ({"optional_outputs": [], "mandatory_outputs": ["test1"]}, "1\n", False),
        ({"optional_outputs": [], "mandatory_outputs": ["test2"]}, "2\n", False),
        # testing a mix of both mandatory and optional outputs
        (
            {"optional_outputs": ["test1"], "mandatory_outputs": ["test2"]},
            "1\n2\n",
            False,
        ),
        (
            {"optional_outputs": ["test2"], "mandatory_outputs": ["test1"]},
            "1\n2\n",
            False,
        ),
        (
            {"optional_outputs": ["all"], "mandatory_outputs": ["test2"]},
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
        # Testing optional outputs and empty mandatory outputs
        ({"optional_outputs": ["all"], "mandatory_outputs": []}, "1\n2\n", True),
        (
            {
                "optional_outputs": ["selective_editing_test1"],
                "mandatory_outputs": [],
            },
            "1\n",
            True,
        ),
        (
            {
                "optional_outputs": ["selective_editing_test2"],
                "mandatory_outputs": [],
            },
            "2\n",
            True,
        ),
        (
            {"optional_outputs": [], "mandatory_outputs": []},
            "No additional_outputs produced\n",
            True,
        ),
        (
            {"optional_outputs": ["all"], "mandatory_outputs": []},
            "No additional_outputs produced\n",
            False,
        ),
        (
            {
                "optional_outputs": [
                    "test1",
                    "test2",
                    "selective_editing_test1",
                    "selective_editing_test2",
                ],
                "mandatory_outputs": [],
            },
            "1\n2\n",
            True,
        ),
        # Testing mandatory outputs and empty optional outputs
        (
            {
                "optional_outputs": [],
                "mandatory_outputs": ["selective_editing_test1"],
            },
            "1\n",
            True,
        ),
        (
            {
                "optional_outputs": [],
                "mandatory_outputs": ["selective_editing_test2"],
            },
            "2\n",
            True,
        ),
        (
            {
                "optional_outputs": [],
                "mandatory_outputs": [
                    "selective_editing_test1",
                    "selective_editing_test2",
                ],
            },
            "1\n2\n",
            True,
        ),
        # Testing a mix of both mandatory and optional outputs
        (
            {
                "optional_outputs": ["selective_editing_test1"],
                "mandatory_outputs": ["selective_editing_test2"],
            },
            "1\n2\n",
            True,
        ),
        (
            {
                "optional_outputs": ["selective_editing_test2"],
                "mandatory_outputs": ["selective_editing_test1"],
            },
            "1\n2\n",
            True,
        ),
        (
            {
                "optional_outputs": ["all"],
                "mandatory_outputs": ["selective_editing_test1"],
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

    with pytest.raises(TypeError):
        get_additional_outputs(
            {
                "optional_outputs": "not_a_list",
                "mandatory_outputs": "also_not_a_list",
            },
            function_mapper,
            pd.DataFrame(),
        )

    with pytest.raises(ValueError):
        get_additional_outputs(
            {"optional_outputs": ["test3"], "mandatory_outputs": []},
            function_mapper,
            pd.DataFrame(),
        )
