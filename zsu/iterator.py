from contextlib import contextmanager
import logging
import warnings

import numpy as np
import pandas as pd

from io import BytesIO, StringIO

logging = logging.getLogger(__name__)

_chunksize = 100


class DataframeReader:
    def __init__(self, chunksize=_chunksize):
        """ead from various input sources and yield DataFrame chunks.

        Arguments:
            chunksize: The maximum number of rows to include in a DataFrame
            chunk.
        """
        # The maximum length of DataFrame returned in the iterator
        self.chunksize = chunksize
        # The total number of rows in the input data
        self.total_rows = None
        # Generate iterable
        self._generate_iter = None

    @property
    def columns(self) -> pd.Index:
        """The columns in the output DataFrame"""
        return self.peek().columns

    @staticmethod
    def slice_generator(length, n):
        """Yields slices for easy chunking of lists, arrays, etc.

        Args:
            length (int): the length of the item to chunk
            n (int): the size of each chunk

        Yields:
            slice: the slice for the next chunk

        Examples:
            >>> [[0, 1, 2, 3, 4, 5][x] for x in slice_generator(6, 2)]
            [[0, 1], [2, 3], [4, 5]]
            >>> list(slice_generator(10, 30))
            [slice(0, 10, None)]
        """

        for i in range(0, length, n):
            yield slice(i, min(i + n, length))

    def from_file(self, path, **kwargs):
        """Iterator from file"""
        self._generate_iter = lambda: iter(pd.read_csv(path, chunksize=self.chunksize, **kwargs))
        return self

    def from_dataframe(self, df: pd.DataFrame):
        """Iterator from DataFrame"""

        def producer():
            slices = self.slice_generator(len(df), self.chunksize)
            for slice_ in slices:
                yield df[slice_]

        self.total_rows = len(df)
        self._generate_iter = producer

        return self

    def pipe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Modify eadh DataFrame chunk before yielding it"""
        df = df.copy()
        return df

    def peek(self) -> pd.DataFrame:
        """The first DataFrame"""
        return next(iter(self))

    def __iter__(self):
        for df in self._generate_iter():
            yield self.pipe(df)


class LineReader:
    def __init__(self, reader: DataframeReader, **kwargs):
        """Generates formatted lines from a source DataframeReader.

        Arguments:
            reader: The source reader. Lines will be generated from the
                DataFrames yielded by this reader.
            kwargs: Arguments passed to ``pd.to_csv``, which controls how the
                lines will be formatted (e.g. the delimiter).
        """
        self.reader = reader
        self.kwargs = kwargs

    def pipe(self, line: str) -> str:
        return line

    def __iter__(self):
        self.kwargs["header"] = self.kwargs.get("header", True)
        for ind, df in enumerate(self.reader):
            if ind == 1:
                self.kwargs["header"] = None
            with StringIO() as f:
                df.to_csv(f, **self.kwargs)
                f.seek(0)
                for line in f:
                    yield self.pipe(line)


class DataframeWriter:
    """A class that writes a series of DataFrames to a variety of outputs"""

    def __init__(self):
        # The value to return
        self.to_return = None
        self.writer = None
        # The context manager
        self._ctm = None
        # The return of the __enter__'ed context manager
        self._sink = None
        # The number of blocks/dataframes written
        self.block_written = 0

    def to_dataframe(self):
        def writer(df):
            self._sink.append(df)

        @contextmanager
        def ctm():
            yield list()
            self.to_return = pd.concat(self._sink)

        self.writer = writer
        self._ctm = ctm()

        return self

    def to_csv(self, path, **kwargs):
        def writer(df):
            if self.block_written > 0:
                kwargs["header"] = None
            pd.to_csv(df, self._sink, **kwargs)

        self._ctm = open(path, "w")
        self.writer = writer
        return self

    def to_parquet(self, path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        self.to_dataframe()
        df_ctm = self._ctm

        @contextmanager
        def ctm():
            with df_ctm as to_yield:
                yield to_yield
            df = self.to_return
            table = pa.Table.from_pandas(df)
            pq.write_table(table, path)

        self._ctm = ctm()
        return self

    def pipe(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def write(self, df: pd.DataFrame):
        # If a context manager is designated, but it's not in a "with" block,
        # it has to be manually entered
        if self._ctm is not None and self._sink is None:
            self.__enter__()

        self.writer(self.pipe(df))
        self.block_written += 1

    def __enter__(self):
        if self._ctm is not None:
            self._sink = self._ctm.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Delegate to the context manager
        if self._ctm is not None:
            self._ctm.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        self.__exit__(None, None, None)
        return self.to_return


class BufferedObjectWriter:

    def __init__(self, writer, chunksize=_chunksize):
        self.writer = writer
        self.chunksize = chunksize

        self.buffer = []
        self.n_in_buffer = 0
        self.n_written = 0
        self.chunk_written = 0

    def write(self, obj):
        if self.n_in_buffer >= self.chunksize:
            self.flush()

        self.buffer.append(obj)
        self.n_in_buffer += 1

    def flush(self):
        if len(self.buffer) == 0:
            return

        self.writer.write(self.buffer)
        self.n_written += self.n_in_buffer
        self.n_in_buffer = 0
        self.chunk_written += 1

        self.buffer = []

    def close(self):
        self.flush()
        return self.writer.close()


class LineWriter(BufferedObjectWriter):

    def __init__(self, writer: DataframeWriter, chunksize=_chunksize, **kwargs):
        super().__init__(writer, chunksize)
        self.kwargs = kwargs
        self.header_lines = 0

    def pipe(self, df):
        return df

    def flush(self):
        if len(self.buffer) == 0:
            return

        df = pd.read_csv(BytesIO(b''.join(self.buffer)), **self.kwargs)

        if self.kwargs.get("index", None) is None:
            idx_start = self.n_written - self.header_lines
            df.index = range(idx_start, idx_start + len(df))

        if self.chunk_written == 0:
            self.kwargs["names"] = df.columns
            self.kwargs["header"] = None
            self.header_lines = self.n_in_buffer - len(df)

        pipe_df = self.pipe(df)
        df = df if pipe_df is None else pipe_df

        self.buffer = df
        super().flush()
