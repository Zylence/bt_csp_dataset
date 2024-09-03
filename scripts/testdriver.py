import logging
import multiprocessing
import os
import queue
import tempfile
from pathlib import Path

from minizinc_wrapper import MinizincWrapper
from parquet import ParquetReader, ParquetWriter
from schemas import Helpers, Schemas, Constants


class Testdriver(MinizincWrapper):

    command_template = ' --solver gecode "{fzn_file}" --json-stream --solver-statistics --input-is-flatzinc'
    errors = 0

    def __init__(self, workload_parquet: Path, output_folder: Path):
        self.workload_parquet = workload_parquet
        self.output_folder = output_folder


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


    @staticmethod
    def worker(job_queue, result_queue, num_total_jobs):
        logger = Testdriver.JobLogger(num_total_jobs)
        wrapper = MinizincWrapper()

        while True:
            try:
                job_num, job = job_queue.get(timeout=10)
            except queue.Empty:
                break


            logger.log(logging.INFO, job_num,
                       f"Processing Variable Ordering {job[Constants.INSTANCE_PERMUTATION]}", job[Constants.PROBLEM_ID])

            fd, temp_file_name = tempfile.mkstemp(suffix=".fzn")
            with open(temp_file_name, 'w') as temp_file:
                temp_file.write(job[Constants.FLAT_ZINC])
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

                        result_queue.put(data)
                        break # todo currently skips nSolutions, do we want to merge it?

            except Exception as e:
                logger.log(logging.ERROR, job_num,
                           f"Validation Error: {e}", job[Constants.PROBLEM_ID])
                Testdriver.errors += 1
                continue



    def run(self, args=None):
        job_reader = ParquetReader(Schemas.Parquet.instances, filename=self.workload_parquet)
        jobs = job_reader.table().to_pandas().to_dict(orient="records")
        logger = Testdriver.JobLogger(len(jobs))

        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()

        logger.log(logging.INFO, 0, "Filling Job Queue")
        for i, job in enumerate(jobs):
            job_queue.put((i+1, job))

        writer = ParquetWriter(Schemas.Parquet.instance_results, self.output_folder)

        num_workers = multiprocessing.cpu_count() // 2
        processes = []

        logger.log(logging.INFO, 0, f"Processing will start using {num_workers} workers.")
        logger.log(logging.INFO, 0, f"Assuming 2s per job, this is going to take {2*len(jobs)/60.0/60.0/num_workers}h")
        for _ in range(num_workers):
            p = multiprocessing.Process(target=Testdriver.worker, args=(job_queue, result_queue, len(jobs)))
            p.start()
            processes.append(p)

        processed_count = 0

        while processed_count < len(jobs):
            output = result_queue.get()
            writer.append_row(output)
            processed_count += 1

            if processed_count % 1000 == 0:
                writer.close_table(f"{self.output_folder.parent}/result_parquet_e{processed_count / 1000}.parquet")
                logger.log(logging.INFO, 0,
                           f"Checkpoint saved after {processed_count} writes.")

        for p in processes:
            p.join()

        writer.close_table(f"{self.output_folder}")
        if Testdriver.errors == 0:
            logger.log(logging.INFO, len(jobs),
                       f"All jobs finished gracefully with {Testdriver.errors} errors.")
        else:
            logger.log(logging.WARN, len(jobs),
                       f"Jobs finished with {Testdriver.errors} errors.") # this means investigate dataset


if __name__ == "__main__":
    temp_dir = tempfile.TemporaryDirectory(delete=False)
    workload_parquet = Path("instances.parquet").resolve()
    result_parquet = Path(f"{temp_dir.name}/result.parquet").resolve()
    testdriver = Testdriver(workload_parquet=workload_parquet, output_folder=result_parquet)
    testdriver.run()