def collate_estimation_data(
    population_frame, sample, period, reference, cell_num, auxiliary, **config
):
    """
    Collate extra data required for estimation.

    Parameters
    ----------
    population_frame: pd.DataFrame
      dataframe containing population frame
    sample: pd.DataFrame
      dataframe containing sample data
    period: Str
      the name of the period column
    cell_num: Str
      the name of the cell number column
    auxiliary: Str
      the name of the auxillary column
    reference: Str
      the name of the reference column
    **config: Dict
      main pipeline configuration. Can be used to input the entire config dictionary

    Returns
    -------
    pd.DataFrame
      population frame containing sampled column

    """

    population_frame = population_frame[[reference, cell_num, auxiliary, period]]
    sample = sample[[reference, period]]

    sample["sampled"] = 1
    # population_frame[
    #     "strata"
    # ] = ""  # Need to figure out what part of cell_num is strata.
    print()

    return population_frame.merge(sample, on=[reference, period], how="left").fillna(
        value={"sampled": 0}
    )
