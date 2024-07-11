import pandas as pd


def pre_processing_imputation(
    population_frame, sample, period, reference, cell_num, auxiliary, **config
):
    """
    Clean and merge JSON inputs.

    Parameters
    ----------
    population_frame: Dict
      raw input as loaded by research_and_development.utils.hdfs_load_json
    sample: Dict
      raw input as loaded by research_and_development.utils.hdfs_load_json
    period: Int/Str
      the period of the population frame and sample
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

    population_frame = pd.DataFrame(population_frame[reference, cell_num, auxiliary])
    sample = pd.DataFrame(sample[reference])

    sample["sampled"] = 1
    population_frame["period"] = period
    population_frame[
        "strata"
    ] = ""  # Need to figure out what part of cell_num is strata.

    return population_frame.merge(sample, on=reference, how="left").fillna(
        value={"sampled": 0}
    )
