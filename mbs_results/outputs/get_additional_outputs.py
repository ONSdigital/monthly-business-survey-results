def get_additional_outputs(config: dict, function_mapper: dict) -> None:
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
        Links outputs from user config the with the relevant functions.
        Keys are functions names of the outputs which the user will handle.
        Values are the functions in the source code

    Raises
    ------
    ValueError
        Raises error if additional_outputs doesn't contain a list or it contains
        a function which is not defined in function_mapper.

    Returns
    -------
    None

    Examples
    --------
    >> example_function = print("Hello world)
    >> config = {additional_outputs:["output_name"]}
    >> function_mapper = {output_name : example_function}
    >> get_additional_outputs(config,function_mapper)

    """

    if not isinstance(config["additional_outputs"], list):

        raise ValueError(
            """
In config file additional_outputs must be a list, please use:\n
["all"] to get all outputs\n
[] to get no outputs\n
or a list with the outputs, e.g. ["output_1","output_2"]
                    """
        )

    if not config["additional_outputs"]:
        print("No additional_outputs produced")
        return None

    if config["additional_outputs"] == ["all"]:

        functions_to_run = function_mapper.keys()

    else:
        functions_to_run = config["additional_outputs"]

    for function in functions_to_run:

        if function in function_mapper:

            function_mapper[function](**config)

        else:
            raise ValueError(
                f"""
    The function {function} is not registerd, check spelling.\n
    Currently the registered functions are:\n {function_mapper}
                    """
            )
