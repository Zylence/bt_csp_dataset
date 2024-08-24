from typing import Callable, Any

import pyarrow.parquet as pq
from threading import Lock
import pyarrow as pa
import pandas as pd

"""
threadsafe writing operations on parquet file with strong schema enforcement
"""
class ParquetWriter:

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.table = pa.Table.from_arrays([[] for _ in self.schema], schema=self.schema)
        self.lock = Lock()

    def append_row(self, row_data: pd.DataFrame):
        with self.lock:
            new_row_table = pa.Table.from_pandas(row_data, schema=self.schema)
            combined_table = pa.concat_tables([self.table, new_row_table])
            self.table = combined_table

    def save_table(self, filename: str):
        with self.lock:
            pq.write_table(self.table, filename)


"""
threadsafe reading operations on parquet file with strong schema enforcement
"""
class ParquetReader:

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.table = None

    def load_table(self, filename: str):
        self.table = pq.read_table(filename, schema=self.schema)


"""
threadsafe reading and writing operations on parquet file with strong schema enforcement
"""
class ParquetReadWriter(ParquetWriter, ParquetReader):
    def __init__(self, schema: pa.Schema):
        super().__init__(schema)

    def load_table(self, filename: str):
        with self.lock:
            super().load_table(filename)

    """
    threadsafe reading and writing on the table using callbacks.
    do not use threading / mp in the cb
    do not nest cbs
    """
    def table_synced_access(self, cb: Callable[[pa.Table], Any]):
        with self.lock:
            return cb(self.table)