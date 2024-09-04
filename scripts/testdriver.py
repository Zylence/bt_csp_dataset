import logging
import multiprocessing
import os
import queue
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, overload

import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import shutil

from instance_generator import FlatZincInstanceGenerator
from minizinc_wrapper import MinizincWrapper
from schemas import Helpers, Schemas, Constants


class Testdriver(MinizincWrapper):

    command_template = ' --solver gecode "{fzn_file}" --json-stream --solver-statistics --input-is-flatzinc'
    errors = 0
    job_loading_threshold = 10_000
    backup_threshold = 5_000_000
    result_buffer_size = 10_000
    num_workers = multiprocessing.cpu_count() // 2

    def __init__(self, feature_vector_parquet: Path, workload_parquet: Path, output_folder: Path):
        self.output_folder = output_folder
        self.workload_partitions = pq.ParquetDataset(workload_parquet).files
        self.job_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.job_view = "job_view"
        self.job_counter = 0
        self.con = duckdb.connect(database=':memory:')

        self.con.execute(f"""
            CREATE TEMPORARY VIEW {self.job_view} AS 
            SELECT * FROM read_parquet({self.workload_partitions})
        """)
        self.job_count = self.con.execute(f"""SELECT COUNT(*) FROM {self.job_view}""").arrow().to_pydict()["count_star()"][0]
        vectors = pq.read_table(feature_vector_parquet, schema=Schemas.Parquet.feature_vector).to_pylist()
        self.feature_vectors = {}
        for vector in vectors:
            self.feature_vectors[vector[Constants.PROBLEM_ID]] = vector


    class JobLogger:
        def __init__(self, total_num_jobs: int):
            logger = multiprocessing.get_logger()
            handler = logging.FileHandler('testdriver.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(processName)s - Job %(job)s - %(progress)s%% - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            self.logger = logger
            self.total_jobs = total_num_jobs

        def log(self, level, job: int, message: str, job_name: str = ""):
            progress = (job / self.total_jobs) * 100
            extra = {'job': f"{job_name} {job}", 'progress': f"{progress:.2f}"}
            self.logger.log(level, message, extra=extra)

    def worker(self):
        logger = Testdriver.JobLogger(self.job_count)
        wrapper = MinizincWrapper()

        while True:
            try:
                job_num, job = self.job_queue.get(timeout=10)
            except queue.Empty:
                break

            logger.log(logging.INFO, job_num,
                       f"Processing Variable Ordering {job[Constants.INSTANCE_PERMUTATION]}", job[Constants.PROBLEM_ID])

            associated_feature_vector = self.feature_vectors[job[Constants.PROBLEM_ID]]
            mutated_zinc = FlatZincInstanceGenerator.substitute_variables(associated_feature_vector[Constants.FLAT_ZINC], job[Constants.INSTANCE_PERMUTATION])

            fd, temp_file_name = tempfile.mkstemp(suffix=".fzn")
            with open(temp_file_name, 'w') as temp_file:
                temp_file.write(mutated_zinc)
            args = Testdriver.command_template.format(fzn_file=temp_file_name)
            _, output = wrapper.run(args)
            os.close(fd)
            Path(temp_file_name).unlink()

            try:

                for o in output:
                    if Constants.SOLVER_STATISTICS in o:
                        data = Helpers.json_to_solution_statistics_dict(o) # todo search in output
                        data[Constants.INSTANCE_PERMUTATION] = "|".join(job[Constants.INSTANCE_PERMUTATION])
                        data[Constants.PROBLEM_ID] = job[Constants.PROBLEM_ID]

                        logger.log(logging.INFO, job_num, f"Backtracks: {data['failures']}, SolveTime: {data['solveTime']}", job[Constants.PROBLEM_ID]) # todo no magic strings

                        self.result_queue.put(data)
                        break # todo currently skips nSolutions, do we want to merge it?

            except Exception as e:
                logger.log(logging.ERROR, job_num,
                           f"Validation Error: {e}", job[Constants.PROBLEM_ID])
                Testdriver.errors += 1
                continue

    def load_next_job_batch(self):
        if len(self.workload_partitions) > 0:
            next = self.workload_partitions.pop()
            jobs = duckdb.execute(f"SELECT * FROM read_parquet('{next}')").arrow().to_pylist()
            for job in jobs:
                self.job_counter += 1
                self.job_queue.put((self.job_counter, job))

    def probe(self, logger: JobLogger):
        # ov every problem fetch one row (one instance)
        db_res = self.con.execute(
            f"""SELECT 
            DISTINCT ON({Constants.PROBLEM_ID}) {Constants.PROBLEM_ID}, {Constants.INSTANCE_PERMUTATION} 
            FROM {self.job_view}""")

        probe_rows = db_res.arrow().to_pylist()

        timings = {}
        for row in probe_rows:
            start = time.time()

            self.job_queue.put((1, row))
            self.worker()
            _ = self.result_queue.get(timeout=0)

            indiv_t = time.time() - start
            logger.log(logging.INFO, 0, f"Probing 1 Job took {indiv_t}s", row[Constants.PROBLEM_ID])

            # get the number of jobs for this problem
            problem_count = self.con.execute(f"""
                SELECT COUNT(*)
                FROM {self.job_view}
                WHERE {Constants.PROBLEM_ID} = '{row[Constants.PROBLEM_ID]}'
            """).arrow().to_pydict()["count_star()"][0]

            timings[row[Constants.PROBLEM_ID]] = (indiv_t, problem_count)

            #timing += (res["solveTime"] + res["initTime"]) # todo no magic strings

        total_t = sum([i for i, _ in timings.values()])
        logger.log(logging.INFO, 0, f"Probing {len(probe_rows)} Jobs took {total_t}s")

        for problem, stats in timings.items():
            estimated_exec_time_for_problem_s = stats[0] * stats[1]     # time with 1 core
            estimated_h = estimated_exec_time_for_problem_s / 60 / 60 / Testdriver.num_workers
            logger.log(logging.INFO, 0, f"Estimating {estimated_h}h with {Testdriver.num_workers} workers.", problem)



    def flush_buffer(self, buffer: list[Dict]):
        table = pa.Table.from_pylist(buffer, schema=Schemas.Parquet.instance_results)
        pq.write_to_dataset(table, root_path=self.output_folder, use_threads=True,
                            schema=Schemas.Parquet.instance_results,
                            partition_cols=[Constants.PROBLEM_ID], existing_data_behavior="overwrite_or_ignore")

    def backup(self, filename: str):
        shutil.make_archive(str(self.output_folder / filename), 'zip', self.output_folder)

    def run(self, args=None):
        logger = Testdriver.JobLogger(self.job_count)
        logger.log(logging.INFO, self.job_counter, "Filling Job Queue with first Batch")

        self.probe(logger)
        self.load_next_job_batch()

        logger.log(logging.INFO, 0, f"Processing will start using {Testdriver.num_workers} workers.")

        processes = []
        for _ in range(Testdriver.num_workers):
            fake_self = SimpleNamespace(
                feature_vectors=self.feature_vectors,
                job_queue=self.job_queue,
                result_queue=self.result_queue,
                job_count=self.job_count
            )
            p = multiprocessing.Process(target=Testdriver.worker, args=(fake_self,))
            p.start()
            processes.append(p)

        processed_count = 0
        buffer = []
        while processed_count < self.job_count:
            output = self.result_queue.get()
            buffer.append(output)
            processed_count += 1

            if len(buffer) % Testdriver.result_buffer_size == 0:
                self.flush_buffer(buffer)
                logger.log(logging.INFO, 0,
                           f"Flushed Parquet Table to disk after {processed_count} jobs.")
                buffer.clear()

            if processed_count % Testdriver.backup_threshold == 0:
                self.backup(f"backup_{processed_count // Testdriver.backup_threshold}")

        if len(buffer) > 0:
            self.flush_buffer(buffer)
            logger.log(logging.INFO, 0,
                       f"Final Flush of Parquet Table to disk.")

        for p in processes:
            p.join()

        if Testdriver.errors == 0:
            logger.log(logging.INFO, processed_count,
                       f"All jobs finished gracefully with {Testdriver.errors} errors.")
        else:
            logger.log(logging.WARN, processed_count,
                       f"Jobs finished with {Testdriver.errors} errors.") # this means investigate dataset


if __name__ == "__main__":
    temp_dir = tempfile.TemporaryDirectory(delete=False)
    feature_vector_parquet = Path("tests/test_data/feature_vector.parquet")
    workload_parquet = Path("instances.parquet").resolve()
    result_parquet = Path(f"{temp_dir.name}/result").resolve()
    testdriver = Testdriver(workload_parquet=workload_parquet, output_folder=result_parquet)
    testdriver.run()