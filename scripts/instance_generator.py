from pathlib import Path

import pandas as pd
import re
import itertools
from minizinc_wrapper import MinizincWrapper
from parquet import ParquetReader, ParquetWriter
from schemas import Constants, Schemas


class FlatZincInstanceGenerator(MinizincWrapper):

    int_search_pattern = re.compile(r"solve\s*::\s*int_search\s*\(\s*(\[.*?\])\s*,", re.DOTALL)
    vars_array_pattern = re.compile(r'\b[\w\[\]]+\b')
    command_template = '"{fzn_file}" --json-stream --model-check-only'
    # command_template = '--json-stream --model-check-only --input-from-stdin --input-is-flatzinc'

    def __init__(self, feature_vector_parquet_input_file: Path, instances_parquet_output_file: Path):
        self.reader = ParquetReader(Schemas.Parquet.feature_vector)
        self.reader.load_table(feature_vector_parquet_input_file.as_posix())
        self.writer = ParquetWriter(Schemas.Parquet.instances)
        self.output_path = instances_parquet_output_file

    def run(self, args=None):
        rows = self.reader.table.select([Constants.PROBLEM_ID, Constants.FLAT_ZINC])
        print(rows)

        processed_results = []
        for i in range(rows.num_rows):
            entry = (rows[0][i].as_py(), rows[1][i].as_py())
            variables = FlatZincInstanceGenerator.extract_variables(entry[1])
            if len(variables) == 0:
                raise Exception(f"No variables could be extracted from {entry}")

            orderings = self.generate_permutations(variables)
            for ordering in orderings:
                ordering_lst = list(ordering)
                new_zinc = FlatZincInstanceGenerator.substitute_variables(entry[1], ordering_lst)

                # sanity check that the newly generated flatZinc file is not broken - todo do via stdin
                # Very expensive and also a little redundant. todo maybe only check sporadically?
                #try:
                #    fd, temp_file_name = tempfile.mkstemp(suffix=".fzn")
                #    with open(temp_file_name, 'w') as temp_file:
                #        temp_file.write(new_zinc)
                #    code, _ = super().run(FlatZincInstanceGenerator.command_template.format(fzn_file=temp_file_name))
                #    os.close(fd)
                #    if code != 0:
                #        # file will not be deleted if that trows, this is intentional
                #        raise Exception(f"Instance generation failed for {temp_file_name}")
                #    print(f"Verified FlatZinc for Problem {entry[0]} with ordering {ordering_lst}")
                #except:
                #    raise
                #finally:
                #    Path(temp_file_name).unlink()


                processed_results.append({
                    Constants.PROBLEM_ID: entry[0],
                    Constants.INSTANCE_PERMUTATION: ordering_lst,
                    Constants.FLAT_ZINC: new_zinc
                })

        self.writer.append_row(pd.DataFrame(processed_results))
        self.writer.save_table(self.output_path.as_posix())


    @staticmethod
    def extract_variables(fzn_content: str) -> list[str]:
        # Regex pattern to match int_search with different possible formats
        match = FlatZincInstanceGenerator.int_search_pattern.search(fzn_content.replace('\n', ''))

        if not match:
            return []

        variables_str = match.group(1)
        variables = re.findall(FlatZincInstanceGenerator.vars_array_pattern, variables_str)

        return variables

    @staticmethod
    def generate_permutations(variables):
        return list(itertools.permutations(variables))

    @staticmethod
    def substitute_variables(fzn_content: str, variables: list[str]) -> str:
        variables_str = f"[{','.join(variables)}]"
        return FlatZincInstanceGenerator.int_search_pattern.sub(
             f"solve :: int_search({variables_str},", fzn_content
        )
