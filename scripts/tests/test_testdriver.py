import unittest
import multiprocessing
import tempfile
from pathlib import Path

from testdriver import Testdriver
from parquet import ParquetReader
from schemas import Constants, Schemas


class TestTestdriver(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(delete=False)
        self.workload_parquet = Path("test_data/instances_short.parquet").resolve()
        self.result_parquet = Path(f"{self.temp_dir.name}/result.parquet").resolve()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_worker(self):
        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()

        jobs = ParquetReader(Schemas.Parquet.instances)
        jobs.load_table(self.workload_parquet.as_posix())
        job_list = jobs.table.to_pandas().to_dict(orient="records")

        for job in job_list:
            job_queue.put(job)

        Testdriver.worker(job_queue, result_queue)

        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        self.assertGreater(len(results), 0)
        for result_df in results:
            self.assertIn(Constants.INSTANCE_RESULTS, result_df.columns)
            self.assertEqual(len(result_df), 1)

    def test_main(self):
        testdriver = Testdriver(workload_parquet=self.workload_parquet, output_folder=self.result_parquet)
        testdriver.run()

        result_reader = ParquetReader(Schemas.Parquet.instance_results)
        result_reader.load_table(self.result_parquet.as_posix())
        result_table = result_reader.table.to_pandas()

        self.assertGreater(len(result_table), 0)



if __name__ == "__main__":
    unittest.main()
