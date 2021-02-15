from datetime import time

import pandas as pd


def day_deduplicate(df_):
    df_copy = df_.copy()
    df_copy["should_keep"] = True
    n_rows = len(df_copy.index)
    for index, row in df_copy.iterrows():
        position = df_copy.index.get_loc(index)
        if position + 1 == n_rows:
            break

        if index.date() == df_copy.index[position + 1].date():
            df_copy.loc[index, "should_keep"] = False

    df_copy = df_copy[df_copy["should_keep"]]
    df_copy = df_copy.drop(["should_keep"], axis=1)
    return df_copy


def is_daily_data(df_):
    times = pd.Series(df_.index).apply(lambda x: x.time()).unique()
    if len(times) == 1 and times[0] == time(0, 0):
        return True
    return False


def fill_void(df_, interval, end):
    n_rows = len(df_.index)
    missing_lines = pd.DataFrame()
    for index, row in df_.iterrows():
        position = df_.index.get_loc(index)
        if position + 1 == n_rows:
            if index < end:
                n_missing = (end - index) // interval
                missing_days = [index + (i + 1) * interval for i in range(n_missing)]
                missing_lines = pd.concat(
                    [missing_lines, pd.DataFrame(row.to_dict(), index=missing_days)]
                )
            break

        step = (df_.index[position + 1] - index).to_pytimedelta()
        if step != interval:
            n_missing = (step // interval) - 1
            missing_days = [index + (i + 1) * interval for i in range(n_missing)]
            missing_lines = pd.concat(
                [missing_lines, pd.DataFrame(row.to_dict(), index=missing_days)]
            )

    df_ = pd.concat([df_, missing_lines])
    df_.sort_index(inplace=True)
    return df_
