import glob
import os
from typing import Generator

from pandas import DataFrame, read_csv

from rxtrade.model import DataFile


def get_data_files(glob_pattern: str) -> Generator[DataFile, None, None]:
    for file_path in glob.glob(glob_pattern):
        ticker = os.path.basename(os.path.dirname(file_path))

        yield DataFile(file_path=file_path, ticker=ticker)


def get_unified_df(glob_pattern: str, ticker: str, period: int) -> DataFrame:
    df = None

    for data_file in get_data_files(glob_pattern):
        if data_file.period != period or data_file.ticker != ticker:
            continue

        df_current = read_csv(data_file.file_path)
        if df is None:
            df = df_current
        else:
            df = df.merge(
                df_current,
                on=["time", "open", "high", "low", "close", "Volume", "Volume MA"],
                how="outer",
                copy=False
            )

    return df
