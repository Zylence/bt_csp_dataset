import math
from pathlib import Path
from typing import Dict

import pyarrow as pa
import pyarrow.parquet as pq
import re
import itertools

from minizinc_wrapper import MinizincWrapper
from schemas import Constants, Schemas


class FlatZincInstanceGenerator:
    int_search_pattern = re.compile(r"solve\s*::\s*int_search\s*\(\s*(\[\s*.*?\s*\]|\w+)\s*,", re.DOTALL)
    array_declaration_pattern = re.compile(r"array\s*\[\d+\.\.\d+\]\s*of\s*var\s*int:\s*(\w+)\s*=\s*(\[.*?\]);",
                                           re.DOTALL)
    vars_array_pattern = re.compile(r'\b[\w\[\]]+\b')
    command_template = ' --json-stream --model-check-only --input-from-stdin --input-is-flatzinc'
    result_buffer_size = 100_000

    def __init__(self, feature_vector_parquet_input_file: Path, instances_parquet_output: Path, max_vars: int):
        self.reader = pq.ParquetReader()
        self.reader.open(feature_vector_parquet_input_file)
        self.output_folder = instances_parquet_output
        self.max_vars = max_vars

    def probe(self):
        rows = self.reader.read_all().select([Constants.PROBLEM_ID, Constants.FLAT_ZINC]).to_pylist()
        for row in rows:
            print(f"Probing {row[Constants.PROBLEM_ID]}")
            variables = FlatZincInstanceGenerator.extract_variables(row[Constants.FLAT_ZINC])
            if len(variables) == 0:
                print(f"WARN No variables could be extracted flatzinc \n {row[Constants.FLAT_ZINC]}")
            print(f"Extracted variables {variables}")
            new_var_ordering = variables.copy()
            new_var_ordering.reverse()

            new_zinc = FlatZincInstanceGenerator.substitute_variables(row[Constants.FLAT_ZINC], new_var_ordering)
            print(f"Generated new flatzinc with variable ordering {new_var_ordering}")

            code, stdout = MinizincWrapper.run(FlatZincInstanceGenerator.command_template, stdin=new_zinc)
            if code != 0:
                # file will not be deleted if that trows, this is intentional
                print(f"WARN Variable substitution failed")

            new_var_ordering_extracted = FlatZincInstanceGenerator.extract_variables(new_zinc)

            if new_var_ordering_extracted != new_var_ordering:
                print(f"WARN Instance generation sanity check failed")
            else:
                print(f"Instance generation successful for {row[Constants.PROBLEM_ID]}")

    def run(self):
        rows = self.reader.read_all().select([Constants.PROBLEM_ID, Constants.FLAT_ZINC])

        self.probe()

        row_num = 0
        buffer = []
        for i in range(rows.num_rows):
            problem_id, fzn_content = rows[0][i].as_py(), rows[1][i].as_py()
            variables = FlatZincInstanceGenerator.extract_variables(fzn_content)
            if len(variables) == 0:
                raise Exception(f"No variables could be extracted from {problem_id} with flatzinc {fzn_content}")
            elif len(variables) > self.max_vars:
                print(f"WARN: SKIPPING {problem_id} because it has too many deciding variables {len(variables)}")
                continue
            print(f"INF: Generating {math.factorial(len(variables))} permutations for {problem_id}")

            orderings = self.generate_permutations(variables)
            for num, ordering in enumerate(orderings):
                ordering_lst = list(ordering)

                buffer.append({
                    Constants.PROBLEM_ID: problem_id,
                    Constants.ROW_NUM: row_num,
                    Constants.INSTANCE_PERMUTATION: ordering_lst,
                })
                row_num += 1

                if num % FlatZincInstanceGenerator.result_buffer_size == 0:
                    print(f"Currently at {num} / {len(orderings)} permutations.")
                    self.flush_buffer(buffer)
                    buffer.clear()

        if len(buffer) > 0:
            self.flush_buffer(buffer)

        self.reader.close()

    def flush_buffer(self, buffer: list[Dict]):
        table = pa.Table.from_pylist(buffer, schema=Schemas.Parquet.instances)
        pq.write_to_dataset(table, root_path=self.output_folder, use_threads=True,
                            schema=Schemas.Parquet.instances,
                            partition_cols=[Constants.PROBLEM_ID], existing_data_behavior="overwrite_or_ignore")

    """
    Returns the list of variables extracted from the int_search annotation. 
    In special cases, where the input variables are defined in other places of the file, also returns
    the array name they are defined in. 
    """
    @staticmethod
    def extract_variables(fzn_content: str) -> list[str]:
        # First, try to find a direct int_search pattern
        match = FlatZincInstanceGenerator.int_search_pattern.search(fzn_content.replace('\n', ''))

        if not match:
            return []

        array_or_var = match.group(1).strip()

        # If the match is an array (starts with '['), extract variables directly
        if array_or_var.startswith('['):
            variables = re.findall(FlatZincInstanceGenerator.vars_array_pattern, array_or_var)
            return variables
        else:
            array_name = array_or_var
            array_pattern = re.compile(rf"{re.escape(array_name)}[^;]*=\s*\[(.*?)\];", re.DOTALL)

            array_match = array_pattern.search(fzn_content.replace('\n', ''))

            if array_match:
                array_content = array_match.group(1)
                variables = re.findall(FlatZincInstanceGenerator.vars_array_pattern, array_content)
                return variables

        return []

    @staticmethod
    def generate_permutations(variables):
        return list(itertools.permutations(variables))

    @staticmethod
    def substitute_variables(fzn_content: str, variables: list[str]) -> str:
        variables_str = f"[{','.join(variables)}]"

        match = FlatZincInstanceGenerator.int_search_pattern.search(fzn_content.replace('\n', ''))

        if not match:
            return fzn_content # todo fatal handle?

        array_or_var = match.group(1).strip()  # either anonymous array [ ... ] or named array

        # array is anonymous
        if array_or_var.startswith('['):
            return FlatZincInstanceGenerator.int_search_pattern.sub(
                f"solve :: int_search({variables_str},", fzn_content
            )
        # array is named
        else:
            array_pattern = re.compile(rf"({re.escape(array_or_var)}[^;]*=\s*)(\[.*?\])(;)", re.DOTALL)
            fzn_content = array_pattern.sub(rf"\1{variables_str}\3", fzn_content)
            return fzn_content

if __name__ == "__main__":
    generator = FlatZincInstanceGenerator(Path("temp/vector.parquet").resolve(), Path("instances").resolve(), 10)
    generator.run()