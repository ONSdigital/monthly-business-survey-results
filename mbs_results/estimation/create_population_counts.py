import pandas as pd


def calculate_turnover_sum_count(
    df: pd.DataFrame, period, strata, colname, **config
) -> pd.DataFrame:

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
    df: pd.DataFrame, period, strata, **config
) -> pd.DataFrame:

    df_1 = calculate_turnover_sum_count(
        df, period, strata, colname="population", **config
    )

    df_2 = calculate_turnover_sum_count(
        df.loc[df["sampled"]], period, strata, colname="sample", **config
    )
    combined = pd.merge(df_1, df_2, on=[period, strata])
    return combined
