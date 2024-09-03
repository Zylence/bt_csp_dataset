import os
from pathlib import Path
from typing import Callable, Any, Dict, overload, Union, List

import pyarrow
import pyarrow.parquet as pq
from threading import Lock
import pyarrow as pa
import pandas as pd

"""
threadsafe writing operations on parquet file with strong schema enforcement
"""
class ParquetWriter:

    def __init__(self, schema: pa.Schema, filename: Path, overwrite=True):
        self.schema = schema
        self.lock = Lock()
        table = pa.Table.from_arrays([[] for _ in self.schema], schema=self.schema) # dummy header
        if overwrite and filename.exists():
            os.remove(filename.as_posix())
        self.writer = pq.ParquetWriter(filename.as_posix(), schema=self.schema)
        self.writer.write_table(table)

    def append_row(self, chunk: list[Dict]):
        """
        Appends the table which will become permanent after calling close_table.
        :param chunk: data of multiple rows
        """
        with self.lock:
            chunk_df = pd.DataFrame(chunk[:])
            table = pa.Table.from_pandas(chunk_df, schema=self.schema)
            self.writer.write_table(table)

    def close_table(self):
        self.writer.close()



# todo get rid of
"""
threadsafe reading operations on parquet file with strong schema enforcement
"""
class ParquetReader:

    def __init__(self, schema: pa.Schema, filename: Path):
        self.schema = schema
        self.file = pq.ParquetFile(filename)


    def table(self, row_group: int = -1, col_filter: list = None) -> pa.Table:
        if row_group < 0:
            return self.file.read(columns=col_filter)
        return self.file.read_row_group(row_group, columns=col_filter)


    def data(self, row_group: int = -1, col_filter: list = None) -> list[Dict]:
        return self.table(row_group=row_group, col_filter=col_filter).to_pylist()


    def release(self):
        self.file.close(force=True)
