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

    def __init__(self, input_files: list[Path], parquet_output_file: Path):
        self.input_files = input_files
        self.parquet_output = parquet_output_file
        self.writer = pq.ParquetWriter(parquet_output_file, schema=Schemas.Parquet.feature_vector)

    def run(self):
        chunk = []
        for i, file in enumerate(self.input_files):
            #fd, fznfile = tempfile.mkstemp(suffix='.fzn')
            #os.close(fd)
            #fznfile = Path(fznfile).resolve().as_posix()
            fznfile = f"{'.'.join(file.as_posix().split('.')[:-1])}.fzn" # todo move to temp folder, then we ll not have to unlink it.
            args = f' --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{file}" --json-stream --compile'
            ret_code, output_lines = MinizincWrapper.run(args)

            if ret_code != 0:
                raise Exception(f"Feature Extraction failed for file {fznfile}.")

            data = Helpers.json_to_normalized_feature_vector_dict("\n".join(output_lines))

            with open(fznfile, 'r') as f:
                data[Constants.PROBLEM_ID] = fznfile.split("/")[-1]  # todo pass name or id in constructor of class
                data[Constants.FLAT_ZINC] = f.read()
            Path(fznfile).resolve().unlink()

            chunk.append(data)

        table = pa.Table.from_pylist(mapping=chunk, schema=Schemas.Parquet.feature_vector)
        self.writer.write_table(table)
        self.writer.close()