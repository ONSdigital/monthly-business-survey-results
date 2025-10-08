import pandas as pd

from mbs_results.utilities.outputs import write_csv_wrapper


def calculate_turnover_sum_count(
    df: pd.DataFrame, period: str, strata: str, colname: str, **config
) -> pd.DataFrame:
    """
    Calculates turnover sum and count and returns an aggregated dataframe
    with the given column name prefixed to the sum and count columns

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe containing frotover. Groups by period and strata
    period : str
        period column name
    strata : str
        strate column name
    colname : str
        column name to prefix to the sum and count columns

    Returns
    -------
    pd.DataFrame
        A grouped dataframe with the sum and count columns prefixed with colname
    """

    df_pop_count = (
        df.groupby([period, strata])
        .agg(summing=("frotover", "sum"), count=("reference", "size"))
        .reset_index()
    )

    df_pop_count.rename(
        columns={"summing": f"{colname}_turnover_sum", "count": f"{colname}_count"},
        inplace=True,
    )

    return df_pop_count



def create_population_count_output(
    df: pd.DataFrame,
    period: str,
    strata: str,
    output_path: str = "",
    save_output: bool = False,
    **config: dict,
) -> pd.DataFrame:
    """
    creates the population count output

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe frotover and sampled. Groups by period and strata
    period : str
        period column name
    strata : str
        strata column name
    output_path : str, optional
        Output path to save dataframe
    save_output : bool, optional
        Default False. If True, saves the output to output_path

    Returns
    -------
    pd.DataFrame
        A grouped dataframe with the sum and count columns prefixed with colname.
        Contains both population and sampled sum and counts for output.
        Returns none if save_output is True
    """
    #KM REMOVE - FIND CODE FOR PREV_PERIOD  
    current_period = int(config["current_period"])
    previous_period = 202201

    print("unique periods in df:", df[period].unique())
    print("current eriod:", current_period)
    print("previous period:", previous_period)
    print("rows matching prev period:", df[df[period] == previous_period].shape[0])
    print("rows matching curremt period:", df[df[period] == current_period].shape[0])
    
    df_prev = df[df[period] == previous_period]
    df_curr = df[df[period] == current_period]

    population_prev = calculate_turnover_sum_count(df_prev, period, strata, "population", **config)
    population_curr = calculate_turnover_sum_count(df_curr, period, strata, "population", **config)

    sample_prev = calculate_turnover_sum_count(df_prev[df_prev["is_sampled"]], period, strata, "sample", **config)
    sample_curr = calculate_turnover_sum_count(df_curr[df_curr["is_sampled"]], period, strata, "sample", **config)

    population_prev = population_prev.drop(columns=[period])
    population_curr = population_curr.drop(columns=[period])
    sample_prev = sample_prev.drop(columns=[period])
    sample_curr = sample_curr.drop(columns=[period])

    prev = pd.merge(population_prev, sample_prev, on=[strata], how="outer").fillna(0)
    curr = pd.merge(population_curr, sample_curr, on=[strata], how="outer").fillna(0)

    

    prev = prev.rename(columns={
        "population_turnover_sum": "population_turnover_previous",
        "population_count": "population_count_previous",
        "sample_turnover_sum": "sample_turnover_previous",
        "sample_count": "sample_count_previous",
    })

    curr = curr.rename(columns={
        "population_turnover_sum": "population_turnover_current",
        "population_count": "population_count_current",
        "sample_turnover_sum": "sample_turnover_current",
        "sample_count": "sample_count_current",
    })

    combined = pd.merge(curr, prev, on=[strata], how="outer").fillna(0)

    combined["population_turnover_difference"] = combined["population_turnover_current"] - combined["population_turnover_previous"]
    combined["sample_turnover_difference"] = combined["sample_turnover_current"] - combined["sample_turnover_previous"]
    combined["population_count_difference"] = combined["population_count_current"] - combined["population_count_previous"]
    combined["sample_count_difference"] = combined["sample_count_current"] - combined["sample_count_previous"]


    if save_output:
        write_csv_wrapper(
            combined,
            output_path + "population_counts.csv",
            config["platform"],
            config["bucket"],
            index=False,
        )

        return
    else:
        return combined
