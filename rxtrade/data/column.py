from datetime import timedelta

import numpy as np
from pandas import DataFrame, to_datetime
import talib


def add_basic_columns(df: DataFrame):
    df["time"] = to_datetime(df["time"])
    df["Market Date"] = to_datetime(np.where(
        df["time"].dt.hour < 17,
        df["time"].dt.date,
        df["time"].dt.date + timedelta(days=1)
    ))
    df["Market Hour"] = df["time"].dt.hour
    df["Market Code"] = np.where(
        ((df["time"].dt.hour == 8) & (df["time"].dt.minute >= 30)) |
        ((df["time"].dt.hour >= 9) & (df["time"].dt.hour < 15)),
        "R",
        "E"
    )


def add_amplitude_column(ticker: str, df: DataFrame):
    df[f"Amplitude ({ticker})"] = abs(df["high"] - df["low"])
    df[f"Amplitude ({ticker} - 5)"] = talib.EMA(df[f"Amplitude ({ticker})"])
