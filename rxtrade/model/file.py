from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(kw_only=True)
class DataFile:
    ticker: str
    file_path: str

    start_date: datetime = field(init=False)
    end_date: datetime = field(init=False)
    period: int = field(init=False)

    def __post_init__(self):
        start_date, end_date, period = Path(self.file_path).stem.split("-")

        self.start_date = datetime.strptime(start_date, "%Y%m%d")
        self.end_date = datetime.strptime(end_date, "%Y%m%d")
        self.period = int(period)
