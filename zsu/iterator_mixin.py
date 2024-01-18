import typing as T
from pathlib import Path

import pandas as pd

from .iterator import DataframeReader, DataframeWriter


class IteratorMixin:
    def __init__(self):
        self._dataframe_reader: T.Union[None, DataframeReader] = None
        self._dataframe_writer: T.Union[None, DataframeWriter] = None

        # The factories for instantiating the reader/writer
        self._dataframe_reader_factory = lambda: DataframeReader(chunksize=100)
        self._dataframe_writer_factory = lambda: DataframeWriter()

        self.input_file = None
        self.output_file = None

    # ------------------ Reader ------------------
    def from_csv(self, input_file, dtype=str, **kwargs):
        self._dataframe_reader = (
            self._dataframe_reader_factory()
            .from_file(input_file, dtype=dtype, **kwargs)
        )
        self.input_file = Path(input_file).resolve()
        return self

    def from_file(self, input_file, **kwargs):
        return self.from_csv(input_file, **kwargs)

    def from_dataframe(self, df: pd.DataFrame):
        self._dataframe_reader = (
            self._dataframe_reader_factory()
            .from_dataframe(df)
        )
        return self

    def to_csv(self, path, index=False, **kwargs):
        self._dataframe_writer = (
            self._dataframe_writer_factory()
            .to_csv(path, index=None, **kwargs)
        )
        output_dir = path.parent.resolve()
        self.output_file = output_dir / path.name

        return self

    def to_file(self, path):
        return self.to_csv(path, index=False)

    def to_parquet(self, path):
        self.output_file = Path(path)
        self._dataframe_writer = (
            self._dataframe_writer_factory()
            .to_parquet(path)
        )
        return self

    def to_dataframe(self):
        self._dataframe_writer = (
            self._dataframe_writer_factory()
            .to_dataframe()
        )
        return self
