import logging
import os
import unittest
import multiprocessing
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
from testdriver import Testdriver
from schemas import Constants, Schemas


class TestTestdriver(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workload_parquet = Path("test_data/testdriver_workload").resolve()
        self.feature_vector_parquet = Path("test_data/feature_vector.parquet").resolve()
        self.output_parquet = Path(f"{self.temp_dir.name}").resolve()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_worker(self):
        td = Testdriver(feature_vector_parquet=self.feature_vector_parquet, workload_parquet_folder=self.workload_parquet, output_folder=self.output_parquet)
        td.load_next_job_batch()
        td.worker()

        results = []
        while not td.result_queue.empty():
            results.append(td.result_queue.get())

        self.assertGreater(len(results), 0)

    def test_main(self):
        testdriver = Testdriver(feature_vector_parquet=self.feature_vector_parquet, workload_parquet_folder=self.workload_parquet, output_folder=self.output_parquet)
        testdriver.run()

        ds = pq.ParquetDataset(self.output_parquet, schema=Schemas.Parquet.instance_results)
        result_table = ds.read()

        self.assertGreater(len(result_table), 0)


if __name__ == "__main__":
    unittest.main()

