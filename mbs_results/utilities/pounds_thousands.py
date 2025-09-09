from __future__ import annotations

from typing import Sequence, Union

import pandas as pd


def create_pounds_thousands_column(
    df: pd.DataFrame,
    *,
    question_col: str = "questioncode",
    source_col: str = "adjustedresponse",
    dest_col: str = "adjustedresponse_pounds_thousands",
    questions_to_apply: Union[Sequence[int], Sequence[str], None] = None,
    ensure_at_end: bool = False,
) -> pd.DataFrame:
    """
    Create a new column 'adjustedresponse_pounds_thousands' with adjustedresponse
    converted to £ thousands (÷ 1000) for selected questions.

    behaviour:
    - No rounding.
    - Question 110: copy mumeric value as-is (i.e., no division by 1000)
    - Question 146: copy text value as-is
    - Only rows whose `question_col` is in `questions_to_apply` are converted;
      other rows in `dest_col` will be just copy the source_col.
    - If `ensure_at_end` is True, `dest_col` is moved to the end of the columns.

    Returns updated dataframe.
    """

    df = df.copy()

    df[question_col] = pd.to_numeric(df[question_col], errors="coerce").astype("int")

    df[dest_col] = df[source_col]

    if questions_to_apply:
        qset = set(str(q) for q in questions_to_apply)
        print(f"qset: {qset}")

        # mask for turnover questions
        mask_turnover = df[question_col].astype(str).isin(qset)

        df.loc[mask_turnover, dest_col] = (
            pd.to_numeric(df.loc[mask_turnover, source_col], errors="coerce") / 1000.0
        )

    # Question 110: copy as-is (no division by 1000)
    mask_110 = df[question_col].astype(str) == "110"
    df.loc[mask_110, dest_col] = df.loc[mask_110, source_col]

    # Question 146: copy text as-is (skip numeric coercion)
    mask_146 = df[question_col].astype(str) == "146"
    df.loc[mask_146, dest_col] = df.loc[mask_146, source_col]

    if ensure_at_end:
        cols = [col for col in df.columns if col != dest_col] + [dest_col]
        df = df.reindex(columns=cols)
    return df
