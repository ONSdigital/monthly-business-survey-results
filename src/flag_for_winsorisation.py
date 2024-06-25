def winsorisation_flag(df, a_weight, g_weight):

    """
    Function to create a column to flag whether or not a row should have
    winsorisation applied.
    Function requires a_weight and g_weight columns produced
    by the estimation method.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a weights and g weights supplied through
        estimation.

    a_weight: float
        Column containing the a weights.
    g_weight: float
        Column containing the g weights.

    Returns
    -------
    pd.DataFrame
        Dataframe with an additional column (nw_ag_flag) that indicates if
        winsorisation should be applied.
    """

    df["flag_calculation"] = df["a_weight"] * df["g_weight"]

    df["nw_ag_flag"] = df["flag_calculation"].apply(lambda x: 1 if x <= 1 else 0)

    df = df.drop("flag_calculation", axis=1)

    return df
