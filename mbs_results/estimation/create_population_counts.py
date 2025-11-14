import pandas as pd

from mbs_results.utilities.inputs import read_csv_wrapper
from mbs_results.utilities.outputs import write_csv_wrapper
from mbs_results.utilities.utils import get_versioned_filename


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
    output_path: str,
    run_id: int,
    platform: str,
    bucket: str,
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
    run_id : int
        Unique run id of the run
    save_output : bool, optional
        Default False. If True, saves the output to output_path

    Returns
    -------
    pd.DataFrame
        A grouped dataframe with the sum and count columns prefixed with colname.
        Contains both population and sampled sum and counts for output.
        Returns none if save_output is True
    """

    population = calculate_turnover_sum_count(
        df, period, strata, "population", **config
    )
    sample = calculate_turnover_sum_count(
        df[df["is_sampled"]], period, strata, "sample", **config
    )

    # outer join and filling na to keep cases when population is is non-zero,
    # and nothing is in sample (i.e. cell has not been sampled)
    full_combined = pd.merge(
        population, sample, on=[period, strata], how="outer"
    ).fillna(0)

    write_csv_wrapper(
        full_combined,
        output_path + f"population_counts_{run_id}.csv",
        platform,
        bucket,
        index=False,
    )

    return full_combined


def format_population_counts_mbs(
    period: str,
    strata: str,
    output_path: str,
    run_id: int,
    platform: str,
    bucket: str,
    current_period: str,
    **config: dict,
):
    """
    produces the formatted population counts for mbs pipeline.
    Loads the full

    Parameters
    ----------
    period : str
        period column name
    strata : str
        strata column name
    output_path : str
        output path where population_counts.csv has been stored
    run_id : int
        Unique run id of the run
    platform : str
        platform name, either "s3" or "network"
    bucket : str
        bucket name when loading from "s3"
    current_period : str
        current period in "YYYYMM" format

    Returns
    -------
    tuple of (pd.DataFrame, str)
        returns the formatted dataframe and the filename including current and previous
        period in filename
    """
    population_counts_filename = get_versioned_filename(
        config["population_counts_prefix"], run_id
    )
    population_counts_path = f"{output_path}{population_counts_filename}"

    df = read_csv_wrapper(
        filepath=population_counts_path,
        import_platform=platform,
        bucket_name=bucket,
    )

    # Better way to do this?
    current_period = pd.to_datetime(current_period, format="%Y%m")
    previous_period = int((current_period - pd.DateOffset(months=1)).strftime("%Y%m"))
    current_period = int(current_period.strftime("%Y%m"))

    df_prev = df[df[period] == previous_period]
    df_curr = df[df[period] == current_period]

    df_prev = df_prev.drop(columns=[period])
    df_curr = df_curr.drop(columns=[period])

    prev = df_prev.rename(
        columns={
            "population_turnover_sum": "population_turnover_sum_previous",
            "population_count": "population_count_previous",
            "sample_turnover_sum": "sample_turnover_sum_previous",
            "sample_count": "sample_count_previous",
        }
    )

    curr = df_curr.rename(
        columns={
            "population_turnover_sum": "population_turnover_sum_current",
            "population_count": "population_count_current",
            "sample_turnover_sum": "sample_turnover_sum_current",
            "sample_count": "sample_count_current",
        }
    )

    combined = pd.merge(curr, prev, on=strata, how="outer").fillna(0)

    combined["population_turnover_difference"] = (
        combined["population_turnover_sum_current"]
        - combined["population_turnover_sum_previous"]
    )
    combined["sample_turnover_difference"] = (
        combined["sample_turnover_sum_current"]
        - combined["sample_turnover_sum_previous"]
    )
    combined["population_count_difference"] = (
        combined["population_count_current"] - combined["population_count_previous"]
    )
    combined["sample_count_difference"] = (
        combined["sample_count_current"] - combined["sample_count_previous"]
    )

    filename = f"mbs_population_counts_{run_id}_{current_period}_{previous_period}.csv"
    return (combined, filename)
