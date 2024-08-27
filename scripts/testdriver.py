import logging
import multiprocessing
import os
import queue
import tempfile
from pathlib import Path

import pandas as pd
from referencing import jsonschema

from minizinc_wrapper import MinizincWrapper
from parquet import ParquetReader, ParquetWriter
from schemas import Helpers, Schemas, Constants


class Testdriver(MinizincWrapper):

    command_template = ' --solver gecode "{fzn_file}" --json-stream --solver-statistics --input-is-flatzinc'

    def __init__(self, workload_parquet: Path, output_folder: Path):
        self.workload_parquet = workload_parquet
        self.output_folder = output_folder


    @staticmethod
    def setup_logger():
        logger = multiprocessing.get_logger()
        handler = logging.FileHandler('testdriver.log')
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(processName)s - %(threadName)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    @staticmethod
    def worker(job_queue, result_queue):
        logger = Testdriver.setup_logger()
        wrapper = MinizincWrapper()

        while True:
            try:
                job = job_queue.get(timeout=10)
            except queue.Empty:
                break

            logger.info(f"Processing job {job[Constants.PROBLEM_ID]}")

            fd, temp_file_name = tempfile.mkstemp(suffix=".fzn")
            with open(temp_file_name, 'w') as temp_file:
                temp_file.write(job[Constants.FLAT_ZINC])
            args = Testdriver.command_template.format(fzn_file=temp_file_name)
            _, output = wrapper.run(args)
            os.close(fd)
            Path(temp_file_name).unlink()

            try:
                df = Helpers.json_to_solution_statistics_dataframe(output[2])
                df[Constants.INSTANCE_PERMUTATION] = "|".join(job[Constants.INSTANCE_PERMUTATION])
                df[Constants.PROBLEM_ID] = job[Constants.PROBLEM_ID]
                res = df.to_dict(orient='records')
                df2 = pd.DataFrame([{Constants.INSTANCE_RESULTS: res}])

                result_queue.put(df2)
            except jsonschema.exceptions.ValidationError as e:
                logger.error(f"Validation error for job {job[Constants.PROBLEM_ID]}: {e}")
                continue



    def run(self, args=None):
        job_reader = ParquetReader(Schemas.Parquet.instances)
        job_reader.load_table(self.workload_parquet.as_posix())
        jobs = job_reader.table.to_pandas().to_dict(orient="records")
        logger = Testdriver.setup_logger()

        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()

        for job in jobs:
            job_queue.put(job)

        writer = ParquetWriter(Schemas.Parquet.instance_results)

        num_workers = multiprocessing.cpu_count() // 2
        processes = []

        for _ in range(num_workers):
            p = multiprocessing.Process(target=Testdriver.worker, args=(job_queue, result_queue))
            p.start()
            processes.append(p)

        processed_count = 0

        while processed_count < len(jobs):
            output = result_queue.get()
            writer.append_row(output)
            processed_count += 1

            if processed_count % 1000 == 0:
                writer.save_table(f"{self.output_folder.parent}/result_parquet_e{processed_count / 1000}.parquet")
                logger.info(f"Checkpoint saved after {processed_count} writes.")

        for p in processes:
            p.join()

        writer.save_table(f"{self.output_folder}")
        logger.info("All jobs processed and final checkpoint saved.")

if __name__ == "__main__":
    temp_dir = tempfile.TemporaryDirectory(delete=False)
    workload_parquet = Path("instances.parquet").resolve()
    result_parquet = Path(f"{temp_dir.name}/result.parquet").resolve()
    testdriver = Testdriver(workload_parquet=workload_parquet, output_folder=result_parquet)
    testdriver.run()