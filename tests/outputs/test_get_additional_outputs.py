import pandas as pd
import pytest

from mbs_results.outputs.get_additional_outputs import get_additional_outputs


def test1(**kwargs):
    print(1)  # dummy function


def test2(**kwargs):
    print(2)  # dummy function


def selective_editing_test1(**kwargs):
    print(3)  # dummy function


def selective_editing_test2(**kwargs):
    print(4)  # dummy function


@pytest.fixture(scope="class")
def function_mapper():

    return {
        "test1": test1,
        "test2": test2,
        "selective_editing_test1": selective_editing_test1,
        "selective_editing_test2": selective_editing_test2,
    }


@pytest.mark.parametrize(
    "config, qa_outputs, optional_outputs, selective_editing, expected",
    [
        # run mandatory_outputs only
        ({"mandatory_outputs": ["test1"]}, True, False, False, "1\n"),
        # run optional_outputs only
        ({"mandatory_outputs": ["test1"]}, False, True, False, "2\n"),
        # Nothing to run
        (
            {"mandatory_outputs": []},
            True,
            False,
            False,
            "No additional_outputs produced\n",
        ),
        # run selective editing only
        ({"mandatory_outputs": ["test1"]}, False, False, True, "3\n4\n"),
    ],
)
def test_output(
    capsys,
    function_mapper,
    config,
    qa_outputs,
    optional_outputs,
    selective_editing,
    expected,
):
    """Test that the right functions were run:
    1. qa_outputs true rest false will run only test1
    2. optional_outputs true rest false will run only test2"
    3.selective_editing true rest false will run only selective_editing_test1
    and selective_editing_test2"""

    get_additional_outputs(
        config,
        function_mapper,
        pd.DataFrame(),
        qa_outputs,
        optional_outputs,
        selective_editing,
    )
    out, err = capsys.readouterr()
    assert out == expected


def test_raise_error_not_list(function_mapper):
    """Test if error is raised when user doesn't pass a list"""

    with pytest.raises(TypeError):
        get_additional_outputs(
            {
                "optional_outputs": "not_a_list",
                "mandatory_outputs": "also_not_a_list",
            },
            function_mapper,
            pd.DataFrame(),
            False,
            True,
        )


def test_raise_error_function_not_defined(function_mapper):
    """Test if error is raised when user passes a
    keyword which does not link to a function"""

    with pytest.raises(ValueError):
        get_additional_outputs(
            {"mandatory_outputs": ["test3"]},
            function_mapper,
            pd.DataFrame(),
            True,
            False,
        )
