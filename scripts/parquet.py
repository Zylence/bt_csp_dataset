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

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.__data: list[Dict] = []
        self.lock = Lock()
        self.writer = pq.ParquetWriter(Path("temp/instances_test.parquet").resolve(), schema=self.schema)

    def append_row(self, row_data: Dict):
        """
        Appends the internal data buffer. Changes only become permanent once save_table is called.
        :param row_data: the data of the row to save as python dict
        """
        with self.lock:
            self.__data.append(row_data)

    def append_row_immediately(self, chunk: list[Dict]):
        """
        Appends the internal data buffer. Changes only become permanent once save_table is called.
        :param chunk: data of multiple rows
        """
        with self.lock:
            chunk_df = pd.DataFrame(chunk[:])
            table = pa.Table.from_pandas(chunk_df, schema=self.schema)
            self.writer.write_table(table)

    # todo should be immutable
    @property
    def data(self) -> list[Dict]:
        return self.__data

    def save_table(self, filename: str, chunk_size: int = 10000):
        #with self.lock:
        #    writer = pq.ParquetWriter(filename, schema=self.schema)
        #    for i in range(0, len(self.data), chunk_size):
        #        chunk = self.data[i:i + chunk_size]
        #        chunk_df = pd.DataFrame(chunk)
        #        table = pa.Table.from_pandas(chunk_df, schema=self.schema)
        #        writer.write_table(table)
        #    writer.close()
        self.writer.close()


"""
threadsafe reading operations on parquet file with strong schema enforcement
"""
class ParquetReader:

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.__data = None

    def load(self, filename: str):
        self.__data = pq.read_table(filename, schema=self.schema).to_pylist()

    @property
    def table(self) -> pa.Table:
        return pa.Table.from_pylist(self.data, schema=self.schema)

    @property
    def data(self) -> list[Dict]:
        return self.__data


"""
threadsafe reading and writing operations on parquet file with strong schema enforcement
"""
class ParquetReadWriter(ParquetWriter, ParquetReader):
    def __init__(self, schema: pa.Schema):
        super().__init__(schema)

    def load(self, filename: str):
        with self.lock:
            super().load(filename)

    @overload
    def synced_access(self, cb: Callable[[pa.Table], Any]) -> Any:
        ...

    @overload
    def synced_access(self, cb: Callable[[List[Dict]], Any]) -> Any:
        ...

    def synced_access(self, cb: Union[Callable[[pa.Table], Any], Callable[[List[Dict]], Any]]) -> Any:
        """
        Threadsafe reading and writing on the table using callbacks.
        Do not use threading / multiprocessing in the callback.
        Do not nest callbacks.
        """
        with self.lock:
            if callable(cb):
                # Check the type of cb argument based on its expected input
                if self.is_table_callback(cb):
                    return cb(self.table)
                else:
                    return cb(self.data)
            else:
                raise TypeError("Callback must be a callable.")

    def is_table_callback(self, cb: Callable) -> bool:
        """
        Determine if the callback expects a pa.Table or a list of dicts.
        """
        try:
            cb(self.table)  # Try calling with a pa.Table
            return True
        except Exception:
            return False