import os
import tempfile
from pathlib import Path
from minizinc_wrapper import MinizincWrapper
from parquet import ParquetReadWriter
from schemas import Schemas, Helpers, Constants

"""
Uses MiniZinc to extract the feature vector of a .mzn files. And outputs the parsed feature vector into
a parquet file. 
"""
class FeatureVectorExtractor(MinizincWrapper):

    def __init__(self, input_files: list[Path], parquet_output_file: Path):
        self.input_files = input_files
        self.parquet_output = parquet_output_file
        self.writer = ParquetReadWriter(Schemas.Parquet.feature_vector)
        if parquet_output_file.exists():
            self.writer.load(parquet_output_file.as_posix())
        else:
            self.writer.save_table(parquet_output_file.as_posix())

    def run(self, args=None):
        for file in self.input_files:
            #fd, fznfile = tempfile.mkstemp(suffix='.fzn')
            #os.close(fd)
            #fznfile = Path(fznfile).resolve().as_posix()
            fznfile = f"{".".join(file.as_posix().split(".")[:-1])}.fzn" # todo move to temp folder, then we ll not have to unlink it.
            args = f' --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{file}" --json-stream --compile'
            ret_code, output_lines = super().run(args)

            if ret_code != 0:
                raise Exception(f"Feature Extraction failed for file {fznfile}.")

            data = Helpers.json_to_normalized_feature_vector_dict("\n".join(output_lines))

            with open(fznfile, 'r') as f:
                data[Constants.PROBLEM_ID] = fznfile.split("/")[-1]  # todo pass name or id in constructor of class
                data[Constants.FLAT_ZINC] = f.read()
            Path(fznfile).resolve().unlink()

            self.writer.append_row(data)
            self.writer.save_table(self.parquet_output.as_posix())