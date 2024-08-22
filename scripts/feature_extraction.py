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
            self.writer.load_table(parquet_output_file.as_posix())
        else:
            self.writer.save_table(parquet_output_file.as_posix())

    def run(self, args=None):
        for file in self.input_files:
            fznfile = f"{".".join(file.as_posix().split(".")[:-1])}.fzn" # todo move to own output folder, then we ll not have to unlink it.
            args = f'--no-optimize --feature-vector --output-fzn-to-file {fznfile} "{file}" --json-stream --compile'
            output_lines = super().run(args)
            df = Helpers.json_to_normalized_feature_vector_dataframe("\n".join(output_lines))

            with open(fznfile, 'r') as f:
                df[Constants.FLAT_ZINC] = f.read()
            Path(fznfile).resolve().unlink()

            self.writer.append_row(df)
            self.writer.save_table(self.parquet_output.as_posix())