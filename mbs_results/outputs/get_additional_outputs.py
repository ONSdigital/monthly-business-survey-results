import pandas as pd


def get_additional_outputs(
    config: dict,
    function_mapper: dict,
    additional_outputs_df: pd.DataFrame,
    selective_editing: bool = False,
) -> dict:
    """
    Runs a set of functions as defined in additional_outputs from the config,
    the function names must exist in function_mapper which also has the relevant
    function as value.

    Parameters
    ----------
    config : dict
        Must have additional_outputs which specifies which functions will run,
        also must have any other arguments needed for the functions in function_mapper.

        Additional_outputs accepts ["all"] or [] or list of functions to run.

    function_mapper : dict
        Links functions to get the output with an output name which can be
        defined in config additional_outputs field from a user.

        Keys are functions names of the outputs which the user will handle.
        Values are the functions in the source code.

    additional_outputs_df : pd.DataFrame
        A DataFrame containing the data required for the functions in function_mapper.

    selective_editing : bool, optional
        If True, returns only selective editing outputs. If False, returns
        non-selective editing outputs. Default is False.

    Raises
    ------
    ValueError
        Raises error if additional_outputs doesn't contain a list or contains
        a function which is not defined in function_mapper.

    Returns
    -------
    dict
        Dictionary of additional outputs, with the keys being the names
        of the outputs and the values being the outputs to be exported.

    Examples
    --------
    >> example_function = print("Hello world")
    >> config = {"optional_outputs" : ["output_name"]}
    >> function_mapper = {"output_name" : example_function}
    >> get_additional_outputs(config, function_mapper)
    >>
    >>
    >> example_function = function(argA, argB)
    >> config = {"optional_outputs" : ["example_output"],
    >>           "argA": "valueA",
    >>           "argB": "valueB"}
    >> function_mapper = {"example_output" : example_function}
    >> get_additional_outputs(config, function_mapper)

    """
    additional_outputs = dict()
    for config_list_name in ["optional_outputs", "mandatory_outputs"]:
        if not isinstance(config[config_list_name], list):
            raise TypeError(
                f"""
                In config file {config_list_name} must be a list, please use: \n
                ["all"] to get all outputs\n
                [] to get only mandatory outputs (defined in dev config) (\n
                or a list with the outputs, e.g. ["output_1", "output_2"]
                """  # noqa: E272
            )

    if config["optional_outputs"] == ["all"]:
        # Dont have to worry about mandatory outputs here, all functions from mapper
        # will run. mandatory and optional outputs should be defined in the mapper
        functions_to_run = function_mapper.keys()

    else:
        # If "all" is not specified, use the provided list combining with
        # mandatory outputs
        functions_to_run = sorted(
            list(set(config["optional_outputs"]) | set(config["mandatory_outputs"]))
        )

    if selective_editing:
        functions_to_run = [
            output
            for output in functions_to_run
            if output.startswith("selective_editing_")
        ]
    else:
        functions_to_run = [
            output
            for output in functions_to_run
            if not output.startswith("selective_editing_")
        ]

    if not functions_to_run:
        print("No additional_outputs produced")
        return None

    for function in functions_to_run:

        if function in function_mapper:

            result = function_mapper[function](
                additional_outputs_df=additional_outputs_df, **config
            )
            # Function can return either a tuple of df, name or just a dataframe
            if isinstance(result, tuple):
                df, name = result
            else:
                df, name = result, None
            additional_outputs[function] = (df, name)

        else:
            raise ValueError(
                f"""
                The function {function} is not registered, check spelling.\n
                Currently the registered functions are:\n {function_mapper}
                    """
            )

    return additional_outputs
