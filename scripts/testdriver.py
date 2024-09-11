import bisect
import datetime
import glob
import logging
import multiprocessing
import os
import queue
import tempfile
import threading
import time
import zipfile
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import SimpleNamespace
from typing import Dict

import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import shutil

from instance_generator import FlatZincInstanceGenerator
from minizinc_wrapper import MinizincWrapper
from schemas import Helpers, Schemas, Constants


class Testdriver:

    command_template = ' --solver gecode --json-stream --solver-statistics --input-from-stdin --input-is-flatzinc'
    job_loading_threshold = 100 #10_000
    backup_threshold = 50 #5_000_000
    result_parquet_chunksize = 50 #10_000
    num_workers = multiprocessing.cpu_count() - 2

    def __init__(self, feature_vector_parquet: Path, workload_parquet_folder: Path, output_folder: Path, log_path: Path, backup_path: Path):
        self.output_folder = output_folder
        self.log_path = log_path
        self.backup_path = backup_path
        self.job_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.job_view = "job_view"
        self.con = duckdb.connect(database=':memory:')
        self.failed_jobs_file = self.output_folder / "failed_jobs.txt"

        # create output folder if not exists
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.backup_path, exist_ok=True)

        # create view on parquet input data to read from
        self.con.execute(f"""
            CREATE TEMPORARY VIEW {self.job_view} AS 
            SELECT * FROM '{workload_parquet_folder}/**/*.parquet'
        """)

        # determine starting point of the job_counter
        # job counter will be minimal job id that is not already worked on
        output_file_pattern = f'{output_folder}/**/*.parquet'
        if glob.glob(output_file_pattern):
            self.job_counter = self.con.execute(
                f"""
                SELECT MIN(w.{Constants.ID})
                FROM {self.job_view} w
                LEFT JOIN '{output_file_pattern}' r
                ON w.{Constants.ID} = r.{Constants.ID}
                WHERE r.{Constants.ID} IS NULL
                """).fetchone()[0]
        else:
            self.job_counter = self.con.execute(
                f"""
                    SELECT MIN({Constants.ID})
                    FROM {self.job_view}
                """).fetchone()[0]

        # get amount of jobs
        self.job_count = self.con.execute(f"""SELECT COUNT(*) FROM {self.job_view}""").fetchone()[0]

        # fill a dictionary with the provided feature vectors for quick access
        vectors = pq.read_table(feature_vector_parquet, schema=Schemas.Parquet.feature_vector).to_pylist()
        self.feature_vectors = {}
        for vector in vectors:
            self.feature_vectors[vector[Constants.PROBLEM_NAME]] = vector
            # todo decide if this next step should happen during feature extraction?
            vector[Constants.FLAT_ZINC] = FlatZincInstanceGenerator.ensure_input_order_annotation(vector[Constants.FLAT_ZINC])


    class JobLogger:
        def __init__(self, total_num_jobs: int, log_path: Path):
            logger = multiprocessing.get_logger()
            if not logger.hasHandlers():
                # max 100 logs size 100MB = 10GB diskspace
                handler = RotatingFileHandler(log_path / 'testdriver.log', maxBytes=100*1024*1024, backupCount=100)

                formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(threadName)s - Job %(job)s - %(progress)s%% - %(message)s'
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

    @staticmethod
    def worker(job_queue, result_queue, feature_vectors: dict[Dict], logger, queue_timeout: int):

        while True:
            try:
                job = job_queue.get(timeout=queue_timeout)
            except queue.Empty:
                break

            job_num = job[Constants.ID]
            logger.log(logging.DEBUG, job_num,
                       f"Processing Variable Ordering {job[Constants.INSTANCE_PERMUTATION]}", job[Constants.PROBLEM_NAME])

            try:
                associated_feature_vector = feature_vectors[job[Constants.PROBLEM_NAME]]
                mutated_zinc = FlatZincInstanceGenerator.substitute_variables(
                    associated_feature_vector[Constants.FLAT_ZINC], job[Constants.INSTANCE_PERMUTATION])
                _, output = MinizincWrapper.run(Testdriver.command_template, stdin=mutated_zinc)

                found_statistics = False
                # in the output look for the line that matches the json statistics schema.
                for o in output:
                    try:
                        data = Helpers.json_to_solution_statistics_dict(o)
                        found_statistics = True
                    except:
                        continue

                    data[Constants.INSTANCE_PERMUTATION] = job[Constants.INSTANCE_PERMUTATION]
                    data[Constants.PROBLEM_NAME] = job[Constants.PROBLEM_NAME]
                    data[Constants.ID] = job_num

                    logger.log(logging.INFO, job_num, f"Backtracks: {data[Constants.FAILURES]}, SolveTime: {data[Constants.SOLVE_TIME]}", job[Constants.PROBLEM_NAME])

                    result_queue.put(data)
                    break

                if not found_statistics:
                    raise ValueError(f"No {Constants.SOLVER_STATISTICS} found in output.")

            except Exception as e:
                logger.log(logging.ERROR, job_num,
                           f"Validation Error: {e}", job[Constants.PROBLEM_NAME])
                result_queue.put(job_num)  # communicates a job failure
                continue


    def load_next_job_batch(self):
        """
        Jobs are loaded in chunks, because storing them all in memory would require to much ram.
        """
        # todo will fail if job range of worked on jobs is not continuous (due to earlier errors)
        # but can work with non continuous ranges if no output is already present
        loaded_in_this_batch = 0
        while self.job_counter < self.job_count and loaded_in_this_batch < Testdriver.job_loading_threshold:

            jobs = self.con.execute(f"""
                SELECT * FROM {self.job_view}
                WHERE {Constants.ID} >= {self.job_counter}
                AND {Constants.ID} < {self.job_counter + Testdriver.job_loading_threshold}
                """).arrow().to_pylist()

            loaded_in_this_batch += len(jobs)
            for job in jobs:
                self.job_queue.put(job)
            self.job_counter += loaded_in_this_batch

    def probe(self, logger: JobLogger):
        """
        Samples the jobs and executes one of each problem.
        Collects timing information for estimations.
        Fails early if anything went wrong in job generation.
        :param logger: logger of the caller
        """
        # ov every problem fetch one row (one instance)
        problem_samples = self.con.execute(
            f"""SELECT 
            DISTINCT ON({Constants.PROBLEM_NAME}) {Constants.PROBLEM_NAME}, {Constants.ID}, {Constants.INSTANCE_PERMUTATION} 
            FROM {self.job_view}""")

        probe_rows = problem_samples.arrow().to_pylist()
        result_queue = queue.Queue()
        job_queue = queue.Queue()
        timings = {}
        for row in probe_rows:
            start = time.time()

            job_queue.put_nowait(row)
            self.worker(job_queue, result_queue, self.feature_vectors, logger, 0)
            _ = result_queue.get_nowait()

            indiv_t = time.time() - start
            logger.log(logging.INFO, 0, f"Probing 1 Job took {indiv_t}s", row[Constants.PROBLEM_NAME])

            # get the number of jobs for this problem
            problem_count = self.con.execute(f"""
                SELECT COUNT(*)
                FROM {self.job_view}
                WHERE {Constants.PROBLEM_NAME} = '{row[Constants.PROBLEM_NAME]}'
            """).arrow().to_pydict()["count_star()"][0]

            timings[row[Constants.PROBLEM_NAME]] = (indiv_t, problem_count)

        total_t = sum([i for i, _ in timings.values()])
        logger.log(logging.INFO, 0, f"Probing {len(probe_rows)} Jobs took {total_t}s")

        estimated_total_h = 0
        for problem, stats in timings.items():
            estimated_exec_time_for_problem_s = stats[0] * stats[1]     # time with 1 core
            estimated_h = estimated_exec_time_for_problem_s / 60 / 60 / Testdriver.num_workers
            estimated_total_h += estimated_h
            logger.log(logging.INFO, 0, f"Estimating {estimated_h}h with {Testdriver.num_workers} workers.", problem)

        logger.log(logging.INFO, 0, f"Estimated total {estimated_total_h}h time with {Testdriver.num_workers} workers.")

    def write_parquet(self, buffer: list[Dict], num: int, logger: JobLogger):
        table = pa.Table.from_pylist(buffer, schema=Schemas.Parquet.instance_results)
        pq.write_to_dataset(table=table,
                            root_path=self.output_folder,
                            use_threads=True,
                            schema=Schemas.Parquet.instance_results,
                            file_visitor=lambda x: logger.log(logging.INFO, 0, f"Parquet Writer touched {x.path}"),
                            basename_template=f"part_{{i}}{num}_{datetime.datetime.now():%H-%M-%S-%f}.parquet",  #  {{i}} must be included...
                            partition_cols=[Constants.PROBLEM_NAME],
                            existing_data_behavior="overwrite_or_ignore")

    def backup(self, filename: str):
        with zipfile.ZipFile(str(self.backup_path / filename), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in glob.glob(f"{self.output_folder}/{Constants.PROBLEM_NAME}=*/*.parquet", recursive=True):
                zipf.write(file, arcname=os.path.relpath(file, self.output_folder))

    def run(self):
        logger = Testdriver.JobLogger(self.job_count, self.log_path)

        self.probe(logger)

        logger.log(logging.INFO, self.job_counter, "Filling Job Queue with first Batch")
        self.load_next_job_batch()

        logger.log(logging.INFO, 0, f"Processing will start using {Testdriver.num_workers} workers.")

        threads = []
        for _ in range(Testdriver.num_workers):
            t = threading.Thread(target=Testdriver.worker, kwargs={
                "job_queue": self.job_queue,
                "result_queue": self.result_queue,
                "feature_vectors": self.feature_vectors,
                "logger": logger,
                "queue_timeout": 30}
            )
            t.start()
            threads.append(t)

        def sort_by(d: Dict) -> int:
            return d[Constants.ID]

        failed_jobs = 0
        processed_count = 0
        sorted_buffer = []
        while processed_count < self.job_count:
            output = self.result_queue.get(timeout=60)
            processed_count += 1

            # check for failed job
            if isinstance(output, int):
                failed_jobs += 1
                with open(self.failed_jobs_file, mode="a") as f:
                    f.write(f"{output}\n")
            else:
                bisect.insort(sorted_buffer, output, key=sort_by)

            if processed_count % Testdriver.job_loading_threshold == 0:
                self.load_next_job_batch()
                logger.log(logging.INFO, 0, f"Loaded new batch of jobs.")

            # When the buffer is slightly larger than the chunksize we write we flush to disk
            # This way we will most likely be flushing fully sorted rows.
            # But as we can not be 100% certain, a postprocessing step is still required.
            if len(sorted_buffer) >= int(Testdriver.result_parquet_chunksize * 1.1):
                self.write_parquet(sorted_buffer[:Testdriver.result_parquet_chunksize], processed_count // Testdriver.result_parquet_chunksize,  logger)
                logger.log(logging.INFO, 0,
                           f"Flushed Parquet Table to disk after {processed_count} jobs.")
                sorted_buffer = sorted_buffer[Testdriver.result_parquet_chunksize:]

            if processed_count % Testdriver.backup_threshold == 0:
                self.backup(f"backup_{processed_count // Testdriver.backup_threshold}.zip")

        if len(sorted_buffer) > 0:
            self.write_parquet(sorted_buffer, processed_count // Testdriver.result_parquet_chunksize, logger)
            logger.log(logging.INFO, 0,
                       f"Final Flush of Parquet Table to disk.")

        for t in threads:
            t.join()

        if failed_jobs == 0:
            logger.log(logging.INFO, processed_count,
                       f"All jobs finished gracefully.")
        else:
            logger.log(logging.WARN, processed_count,
                       f" {failed_jobs} Jobs failed.")  # this means investigate dataset


if __name__ == "__main__":
    temp_dir = tempfile.TemporaryDirectory()
    feature_vector_parquet = Path("temp/vector.parquet")
    workload_parquet = Path("instances").resolve()
    result_parquet = Path(f"result").resolve()
    backups = Path(f"backups").resolve()
    testdriver = Testdriver(
        feature_vector_parquet=feature_vector_parquet,
        workload_parquet_folder=workload_parquet,
        output_folder=result_parquet,
        backup_path=backups,
        log_path=backups,
    )
    testdriver.run()
    temp_dir.cleanup()