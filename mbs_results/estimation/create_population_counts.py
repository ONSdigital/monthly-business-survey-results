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

    # Check sampled == 1 is the correct way to filter on sampled
    # if census, then not sampled change column column for "is census",
    # change winsorisation
    df_2 = calculate_turnover_sum_count(
        df.loc[df["sampled"] is True], period, strata, colname="sample", **config
    )
    combined = pd.merge(df_1, df_2, on=[period, strata])
    return combined


if __name__ == "__main__":
    from mbs_results.utilities.inputs import load_config

    config = load_config()
    pop_file_path = "../population_counts_input.csv"
    df = pd.read_csv(pop_file_path)
    output = create_population_count_output(df, **config)
    print(output)

    print("\n Expected output:")
    expected_output = pd.read_csv("../population_counts_output.csv")
    print(expected_output)
