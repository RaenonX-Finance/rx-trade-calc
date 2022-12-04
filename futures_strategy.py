import math
from datetime import datetime
from typing import Literal

import numpy as np
import pandas as pd
from pandas import DataFrame

from rxtrade.data import add_amplitude_column, add_basic_columns
from rxtrade.utils import get_unified_df


def get_result_df(history: list, mult: float, data_df: DataFrame):
    df = DataFrame(history)

    df["change"] = (df["exit"] - df["entry"]) * np.where(df["side"] == "LONG", 1, -1) * mult
    df["accumulated"] = df["change"].cumsum()

    df["win"] = (df["change"] > 0).cumsum()
    df["lose"] = (df["change"] < 0).cumsum()
    df["wr"] = df["win"].cumsum() / (df["win"].cumsum() + df["lose"].cumsum())

    df_temp = df.merge(data_df, left_on="exit time", right_on="time")
    df["valid exit"] = (df_temp["low"] <= df_temp["exit"]) & (df_temp["exit"] <= df_temp["high"])

    df["profit median"] = df[df["change"] > 0]["change"].expanding().median()
    df["loss median"] = df[df["change"] < 0]["change"].expanding().median()

    return df


def get_data_df(ticker: str) -> DataFrame:
    df = get_unified_df("./archive/futures/**/*.csv", ticker, 5)

    add_basic_columns(df)
    add_amplitude_column(ticker, df)

    # df["Wave Min"] = df.iloc[argrelextrema(df["close"].values, np.less_equal, order=3)[0]]["close"]
    # df["Wave Max"] = df.iloc[argrelextrema(df["close"].values, np.greater_equal, order=3)[0]]["close"]

    df["Direction"] = np.where((df["close"] - df["open"]) > 0, "U", "D")
    df["Prev Direction"] = df["Direction"].shift(1)

    df[f"Body / Wick ({ticker})"] = abs(df["close"] - df["open"]) / abs(df["high"] - df["low"])
    df["Prev Open"] = df["open"].shift(1)
    df["Prev High"] = df["high"].shift(1)
    df["Prev Low"] = df["low"].shift(1)
    df["Prev Close"] = df["close"].shift(1)
    df["Prev 2 Open"] = df["open"].shift(2)
    df["Prev 2 High"] = df["high"].shift(2)
    df["Prev 2 Low"] = df["low"].shift(2)
    df["Prev 2 Close"] = df["close"].shift(2)
    df["Prev Big Action"] = df[f"Amplitude ({ticker})"].shift(1) > (df[f"Amplitude ({ticker} - 5)"].shift(1) * 1.3)

    df["Prev Amplitude 5"] = df[f"Amplitude ({ticker} - 5)"].shift(1)

    bp = [np.nan]
    for _, row in df.iloc[1:].iterrows():
        if row["Direction"] != row["Prev Direction"]:
            bp.append(row["Prev Open"])
            continue

        if row["Direction"] == "U":
            bp.append(row["Prev Open"] if row["open"] > bp[-1] else bp[-1])
        else:
            bp.append(row["Prev Open"] if row["open"] < bp[-1] else bp[-1])

    df["Breakpoint"] = bp

    return df


def entry(history: list, time: datetime, side: Literal["LONG", "SHORT"], price: float, note: str = ""):
    history.append({
        "side": side,
        "entry time": time,
        "exit time": None,
        "entry": price,
        "exit": None,
        "entry note": note,
        "exit note": None,
    })


def exit_(history: list, time: datetime, price: float, note: str = ""):
    history[-1]["exit time"] = time
    history[-1]["exit note"] = note
    history[-1]["exit"] = price


def long(history: list, time: datetime, price: float, note: str = ""):
    if history and history[-1]["side"] == "LONG":
        return

    if history and not history[-1]["exit"]:
        exit_(history, time, price, "Reverse")

    entry(history, time, "LONG", price, note)


def short(history: list, time: datetime, price: float, note: str = ""):
    if history and history[-1]["side"] == "SHORT":
        return

    if history and not history[-1]["exit"]:
        exit_(history, time, price, "Reverse")

    entry(history, time, "SHORT", price, note)


def current_side(history: list):
    return "NEUTRAL" if history[-1]["exit"] else history[-1]["side"]


def current_trade(history: list):
    return history[-1]


def current_px_change(history: list, price: float):
    trade = current_trade(history)

    return (price - trade["entry"]) * (1 if trade["side"] == "LONG" else -1)


def reverse_position(history: list, time: datetime, price: float):
    if history[-1]["side"] == "LONG":
        short(history, time, price)
    else:
        long(history, time, price)


def is_prev_up(row):
    return row["Prev Close"] > row["Prev Open"]


def is_prev_down(row):
    return row["Prev Close"] < row["Prev Open"]


def is_prev_reversed(row):
    return row["close"] > row["Breakpoint"] if row["Direction"] == "U" else row["close"] < row["Breakpoint"]


def is_current_over_prev(row, mult: float = 1):
    if not is_prev_reversed(row):
        return False

    match (is_prev_up(row), mult):
        case (True, 1):
            return row["close"] < row["Prev Open"]
        case (False, 1):
            return row["close"] < row["Prev Open"]
        case (True, mult):
            return row["close"] < (row["open"] - abs(row["Prev Close"] - row["Prev Open"]) * mult)
        case (False, mult):
            return row["close"] > (row["open"] + abs(row["Prev Open"] - row["Prev Close"]) * mult)


def strategy(history: list, df: DataFrame):
    for time, row in df.iterrows():
        time: datetime

        if math.isnan(row["Prev Open"]):
            # History #1 - observe
            continue

        if not history:
            # History #2 - initial entry
            if row["Prev Direction"] == "U":
                long(history, time, row["open"], "Initial")
            else:
                short(history, time, row["open"], "Initial")

            continue

        # History #3+
        entry = current_trade(history)["entry"]
        tolerance = row["Prev Amplitude 5"]
        if current_side(history) == "LONG":
            change = current_px_change(history, row["low"])

            if change < 0 and abs(change) > tolerance:
                # Variable amount stop loss
                exit_(history, time, entry - tolerance, "Fixed SL")
            # elif row["low"] < entry:
            #     # Possible break-to-loss
            #     exit_(history, time, row["close"], "B2L")
        elif current_side(history) == "SHORT":
            change = current_px_change(history, row["high"])

            if change < 0 and abs(change) > tolerance:
                # Variable amount stop loss
                exit_(history, time, entry + tolerance, "Fixed SL")
            # elif row["high"] > entry:
            #     # Possible break-to-loss
            #     exit_(history, time, row["close"], "B2L")

        if row["close"] < row["Prev Open"]:
            long(history, time, row["close"])
        elif row["close"] > row["Prev Open"]:
            short(history, time, row["close"])


def main(ticker: str):
    df = get_data_df(ticker)
    df.set_index("time", inplace=True)

    history = []

    strategy(history, df)

    return get_result_df(history, 2, df), df


if __name__ == '__main__':
    # results, data = main("NQ", "archive/futures/NQ/20220130-20220204-5.csv")
    # results, data = main("NQ", "archive/futures/NQ/20211205-20220128-5.csv")

    results, data = main("NQ")

    pd.options.display.max_rows = None
    pd.options.display.max_columns = None
    pd.options.display.width = None
    pd.options.display.max_colwidth = None

    def format_datetime(t):
        if t is pd.NaT:
            return ""

        return t.strftime("%m-%d %H:%M")


    print(results.to_string(formatters={"entry time": format_datetime, "exit time": format_datetime}))

