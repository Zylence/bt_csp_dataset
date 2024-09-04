import logging
import multiprocessing
import os
import queue
import tempfile
import threading
import time
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
from concurrent.futures import ThreadPoolExecutor, as_completed

from schemas import Constants
from testdriver import Testdriver


parquet_file = pq.ParquetFile(Path("temp/instances.parquet").resolve())


#def process_chunk(table):
#    df = table.to_pandas()
#    print(df.head())


all_unique_ids = set()
for i in range(parquet_file.num_row_groups):
    table = parquet_file.read_row_group(i, columns=[Constants.PROBLEM_ID])
    unique_ids_in_group = pc.unique(table.column(0)).to_pylist()
    all_unique_ids.update(unique_ids_in_group)

probed_rows = {}

for i in range(parquet_file.num_row_groups):
    i = i+1
    id_table = parquet_file.read_row_group(i, columns=[Constants.PROBLEM_ID])
    first_row_id = id_table.slice(0, 1).to_pydict()[Constants.PROBLEM_ID][0]

    if first_row_id in all_unique_ids and first_row_id not in probed_rows:
        full_table = parquet_file.read_row_group(i)
        full_row = full_table.slice(0, 1).to_pydict()
        probed_rows[first_row_id] = full_row

    if len(probed_rows) == len(all_unique_ids):
        break

timing = 0
timing_e = 0
jq = queue.Queue()
rq = queue.Queue()
for row_id, row in probed_rows.items():
    start = time.time()
    #fd, filename = tempfile.mkstemp(delete=False, suffix=".parquet")
    #os.close(fd)
    #table = pa.Table.from_pydict(row)
    #pq.write_table(where=filename.name, table=table)
    #td = Testdriver(output_folder=Path('temp/lala.parquet'), workload_parquet=Path(filename.name).resolve())
    row[Constants.PROBLEM_ID] = row[Constants.PROBLEM_ID][0]
    row[Constants.INSTANCE_PERMUTATION] = row[Constants.INSTANCE_PERMUTATION][0]
    row[Constants.FLAT_ZINC] = row[Constants.FLAT_ZINC][0]
    jq.put((1, row))
    Testdriver.worker(job_queue=jq, result_queue=rq, num_total_jobs=1)
    indiv_t = time.time() - start
    print(indiv_t)
    timing_e += (indiv_t)
    while not rq.empty():
        res = rq.get(timeout=0)
        timing += (res["solveTime"] + res["initTime"])
        print(f"ID: {row_id}")
        print(row)
        print("-" * 40)

print(timing / len(probed_rows))
print(timing_e / len(probed_rows))

