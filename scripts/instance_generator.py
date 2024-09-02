import math
from pathlib import Path
from typing import Optional, List, Dict, Generator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import itertools
from minizinc_wrapper import MinizincWrapper
from parquet import ParquetReader, ParquetWriter
from schemas import Constants, Schemas


class FlatZincInstanceGenerator(MinizincWrapper):
    int_search_pattern = re.compile(r"solve\s*::\s*int_search\s*\(\s*(\[\s*.*?\s*\]|\w+)\s*,", re.DOTALL)
    array_declaration_pattern = re.compile(r"array\s*\[\d+\.\.\d+\]\s*of\s*var\s*int:\s*(\w+)\s*=\s*(\[.*?\]);",
                                           re.DOTALL)
    vars_array_pattern = re.compile(r'\b[\w\[\]]+\b')
    command_template = '"{fzn_file}" --json-stream --model-check-only'
    # command_template = '--json-stream --model-check-only --input-from-stdin --input-is-flatzinc'

    def __init__(self, feature_vector_parquet_input_file: Path, instances_parquet_output_file: Path, max_vars: int):
        self.reader = ParquetReader(Schemas.Parquet.feature_vector)
        self.reader.load(feature_vector_parquet_input_file.as_posix())
        self.writer = ParquetWriter(Schemas.Parquet.instances)
        self.output_path = instances_parquet_output_file
        self.max_vars = max_vars

    def run(self, args=None):
        rows = self.reader.table.select([Constants.PROBLEM_ID, Constants.FLAT_ZINC])
        row_data = []
        for i in range(rows.num_rows):
            entry = (rows[0][i].as_py(), rows[1][i].as_py())
            variables, array_name = FlatZincInstanceGenerator.extract_variables(entry[1])
            if len(variables) == 0:
                raise Exception(f"No variables could be extracted from {entry}")
            elif len(variables) > self.max_vars:
                print(f"WARN: SKIPPING {entry[0]} because it has too many deciding variables {len(variables)}")
                continue
            print(f"INF: Generating {math.factorial(len(variables))} permutations for {entry[0]}")

            orderings = self.generate_permutations(variables)
            for num, ordering in enumerate(orderings):
                ordering_lst = list(ordering)
                new_zinc = FlatZincInstanceGenerator.substitute_variables(entry[1], ordering_lst, array_name)

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

                row_data.append({
                    Constants.PROBLEM_ID: entry[0],
                    Constants.INSTANCE_PERMUTATION: ordering_lst,
                    Constants.FLAT_ZINC: new_zinc
                })

                if num % 10000 == 0:
                    print(f"Currently at {num} / {len(orderings)} permutations.")
                    self.writer.append_row_immediately(row_data) # todo only temp
                    row_data = []



        self.writer.save_table(self.output_path.as_posix())


    """
    Returns the list of variables extracted from the int_search annotation. 
    In special cases, where the input variables are defined in other places of the file, also returns
    the array name they are defined in. 
    """
    @staticmethod
    def extract_variables(fzn_content: str) -> (list[str], Optional[str]):
        # First, try to find a direct int_search pattern
        match = FlatZincInstanceGenerator.int_search_pattern.search(fzn_content.replace('\n', ''))

        if not match:
            return [], None

        array_or_var = match.group(1).strip()

        # If the match is an array (starts with '['), extract variables directly
        if array_or_var.startswith('['):
            variables = re.findall(FlatZincInstanceGenerator.vars_array_pattern, array_or_var)
            return variables, None
        else:
            array_name = array_or_var
            array_pattern = re.compile(rf"{re.escape(array_name)}[^;]*=\s*\[(.*?)\];", re.DOTALL)

            array_match = array_pattern.search(fzn_content.replace('\n', ''))

            if array_match:
                array_content = array_match.group(1)
                variables = re.findall(FlatZincInstanceGenerator.vars_array_pattern, array_content)
                return variables, array_name

        return [], None

    @staticmethod
    def generate_permutations(variables):
        return list(itertools.permutations(variables))

    #@staticmethod
    #def substitute_variables(fzn_content: str, variables: list[str]) -> str:
    #    variables_str = f"[{','.join(variables)}]"
    #    return FlatZincInstanceGenerator.int_search_pattern.sub(
    #         f"solve :: int_search({variables_str},", fzn_content
    #    )

    @staticmethod
    def substitute_variables(fzn_content: str, variables: list[str], array_name: str = None) -> str:
        variables_str = f"[{','.join(variables)}]"

        if not array_name:
            # Old format: Substitute directly within the int_search annotation
            return FlatZincInstanceGenerator.int_search_pattern.sub(
                f"solve :: int_search({variables_str},", fzn_content
            )
        else:
            array_pattern = re.compile(rf"({re.escape(array_name)}[^;]*=\s*)(\[.*?\])(;)", re.DOTALL)
            fzn_content = array_pattern.sub(rf"\1{variables_str}\3", fzn_content)
            return fzn_content