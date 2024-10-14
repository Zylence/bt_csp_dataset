import logging
import os
import queue
import unittest
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from testdriver import Testdriver
from schemas import Schemas


class TestTestdriver(unittest.TestCase):

    class NullLogger:
        def log(self, level, job: int, message: str, job_name: str = ""): pass

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workload_parquet = Path("test_data/instances").resolve()
        self.feature_vector_parquet = Path("test_data/feature_vector.parquet").resolve()
        self.output_parquet = Path(f"{self.temp_dir.name}/output").resolve()
        self.no_parquet = Path(f"{self.temp_dir.name}/others").resolve()

    def tearDown(self):
        logging.shutdown()
        self.temp_dir.cleanup()

    def test_worker(self):
        td = Testdriver(feature_vector_parquet=self.feature_vector_parquet,
                        workload_parquet_folder=self.workload_parquet,
                        output_folder=self.output_parquet,
                        backup_path=self.no_parquet,
                        log_path=self.no_parquet
                        )

        td.job_queue = queue.Queue()
        td.result_queue = queue.Queue()
        td.load_next_job_batch(logger=TestTestdriver.NullLogger())

        td.worker(td.job_queue, td.result_queue, td.feature_vectors, TestTestdriver.NullLogger(), 2)

        results = []
        while not td.result_queue.empty():
            results.append(td.result_queue.get())

        self.assertEqual(len(results), 4)

    def test_main(self):
        testdriver = Testdriver(feature_vector_parquet=self.feature_vector_parquet,
                                workload_parquet_folder=self.workload_parquet,
                                output_folder=self.output_parquet,
                                backup_path=self.no_parquet,
                                log_path=self.no_parquet)
        testdriver.run()

        #os.remove(self.output_parquet / "failed_jobs.txt")
        ds = pq.ParquetDataset(self.output_parquet, schema=Schemas.Parquet.instance_results)
        result_table = ds.read()

        self.assertGreater(len(result_table), 0)


if __name__ == "__main__":
    unittest.main()

