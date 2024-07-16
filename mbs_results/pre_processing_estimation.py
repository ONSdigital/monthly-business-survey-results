def derive_estimation_variables(
    population_frame,
    sample,
    calibration_group_map,
    period,
    reference,
    cell_number,
    **config
):
    """
    Derive extra variables required for estimation.

    Parameters
    ----------
    population_frame: pd.DataFrame
        dataframe containing population frame
    sample: pd.DataFrame
        dataframe containing sample data
    calibration_group_map: pd.DataFrame
        dataframe containing map between cell number and calibration group
    period: Str
        the name of the period column
    cell_number: Str
        the name of the cell number column
    reference: Str
        the name of the reference column
    **config: Dict
       main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
        population frame containing sampled column

    """
    population_frame.merge(calibration_group_map, on=[cell_number], how="left")
    # TODO: check if cell_no is needed or should be dropped

    sample = sample[[reference, period]]
    sample["sampled"] = 1

    return population_frame.merge(sample, on=[reference, period], how="left").fillna(
        value={"sampled": 0}
    )
