import multiprocessing
import os
import re
import tempfile
import threading
from pathlib import Path

from minizinc_wrapper import MinizincWrapper
from schemas import Schemas, Helpers, Constants
import pyarrow.parquet as pq
import pyarrow as pa

"""
Uses MiniZinc to extract the feature vector of a .mzn files. And outputs the parsed feature vector into
a parquet file. 
"""
class FeatureVectorExtractor:

    semaphore = threading.Semaphore(multiprocessing.cpu_count())
    command_template = ' --solver cp --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{mzn}" --json-stream --compile'
    # command_template = ' --solver api --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{mzn}" --json-stream --compile'
    # command_template = ' --solver gecode --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{mzn}" --json-stream --compile'

    def __init__(self, input_files: (str, list[Path]), parquet_output_file: Path):
        self.input_files = input_files
        self.parquet_output = parquet_output_file
        self.writer = pq.ParquetWriter(parquet_output_file, schema=Schemas.Parquet.feature_vector)

    @staticmethod
    def worker(queue, tempfolder, problem_id: str, mzn_fqn: Path):
        with FeatureVectorExtractor.semaphore:
            mzn_filename = mzn_fqn.as_posix().split("/")[-1]
            raw_filename = mzn_filename.rstrip(".mzn")
            fznfile_fqn = f"{tempfolder.name}/{raw_filename}.fzn"

            print(f"starting work on {raw_filename}")
            args = FeatureVectorExtractor.command_template.format(fznfile=fznfile_fqn, mzn=mzn_fqn)
            ret_code, output_lines = MinizincWrapper.run(args)

            if ret_code != 0:
                raise Exception(f"Feature Extraction failed for file {fznfile_fqn}.")

            data = Helpers.json_to_normalized_feature_vector_dict("\n".join(output_lines))

            with open(mzn_fqn, 'r') as f:
                data[Constants.MINI_ZINC] = f.read()

            with open(fznfile_fqn, 'r') as f:
                data[Constants.PROBLEM_ID] = problem_id
                data[Constants.MODEL_NAME] = mzn_filename
                data[Constants.FLAT_ZINC] = f.read()

            queue.put(data)
            print(f"{raw_filename} feature extraction is done.")

    def run(self):
        queue = multiprocessing.Queue()
        temp_folder = tempfile.TemporaryDirectory()

        threads = []
        for problem_id, data in self.input_files:
            t = threading.Thread(target=FeatureVectorExtractor.worker, args=(queue, temp_folder, problem_id, data))
            t.start()
            threads.append(t)

        chunk = []
        processed = 0
        while processed < len(self.input_files):
            processed += 1
            chunk.append(queue.get())

        for t in threads:
            t.join()

        def sort_by_problem_id(vektor):
            return int(vektor[Constants.PROBLEM_ID])

        chunk.sort(key=sort_by_problem_id)

        print("Will flush table.")
        table = pa.Table.from_pylist(mapping=chunk, schema=Schemas.Parquet.feature_vector)
        self.writer.write_table(table)
        self.writer.close()
        temp_folder.cleanup()

    @staticmethod
    def input_format_helper(problems: Path) -> (str, list[Path]):
        file_list = []

        for root, dirs, files in os.walk(problems):
            for file in files:
                if file.endswith('.mzn'):
                    parent_dir = os.path.basename(os.path.dirname(root))
                    match = re.match(r'prob(\d+)', parent_dir)
                    problem_id = match.group(1) #int(match.group(1))
                    file_path = Path(os.path.join(root, file)).resolve()
                    file_list.append((problem_id, file_path))

        return file_list

if __name__ == "__main__":
    inp_p = Path("F:/Downloads/Problems_2").resolve()
    inp = FeatureVectorExtractor.input_format_helper(inp_p)
    out_p = Path("D:/Local/projects/python/ba_csp_dataset/scripts/temp/vector_big_10.parquet").resolve()
    fve = FeatureVectorExtractor(input_files=inp, parquet_output_file=out_p)
    fve.run()

