import os
import re
import tempfile
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

    def __init__(self, input_files: (str, list[Path]), parquet_output_file: Path):
        self.input_files = input_files
        self.parquet_output = parquet_output_file
        self.writer = pq.ParquetWriter(parquet_output_file, schema=Schemas.Parquet.feature_vector)

    def run(self):
        chunk = []
        temp_folder = tempfile.TemporaryDirectory()
        for i, data in enumerate(self.input_files):
            problem_id, mzn_fqn = data
            mzn_filename = mzn_fqn.as_posix().split("/")[-1]
            raw_filename = mzn_filename.rstrip(".mzn")
            fznfile_fqn = f"{temp_folder.name}/{raw_filename}.fzn"
            args = f' --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile_fqn} "{mzn_fqn}" --json-stream --compile'
            ret_code, output_lines = MinizincWrapper.run(args)

            if ret_code != 0:
                raise Exception(f"Feature Extraction failed for file {fznfile_fqn}.")

            data = Helpers.json_to_normalized_feature_vector_dict("\n".join(output_lines))

            with open(fznfile_fqn, 'r') as f:
                data[Constants.PROBLEM_ID] = problem_id
                data[Constants.MODEL_NAME] = mzn_filename
                data[Constants.FLAT_ZINC] = f.read()

            chunk.append(data)

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

