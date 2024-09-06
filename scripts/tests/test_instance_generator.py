import math
import os
import unittest
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from instance_generator import FlatZincInstanceGenerator
from schemas import Schemas, Constants


class TestFlatZincInstanceGenerator(unittest.TestCase):

    def test_extract_variables(self):
        test_cases = [
            ('solve :: int_search([x, y],', ['x', 'y']),
            ('solve :: int_search([23, b, c],', ['23', 'b', 'c']),
            ('solve :: int_search([],', []),
            ('solve     ::      int_search([var1, var2, var3, var4],', ['var1', 'var2', 'var3', 'var4']),
            ('array [1..6] of var int: mark:: output_array([1..6]) = [0,X_INTRODUCED_17_,X_INTRODUCED_18_,X_INTRODUCED_19_,X_INTRODUCED_20_,X_INTRODUCED_21_]; \n solve     ::      int_search(mark,', ['0','X_INTRODUCED_17_','X_INTRODUCED_18_','X_INTRODUCED_19_','X_INTRODUCED_20_','X_INTRODUCED_21_'])
        ]

        for fzn_content, expected_vars in test_cases:
            with self.subTest(fzn_content=fzn_content):
                actual_vars = FlatZincInstanceGenerator.extract_variables(fzn_content)
                self.assertEqual(expected_vars, actual_vars)

    def test_generate_permutations(self):
        variables = ['x', 'y']
        expected_permutations = [('x', 'y'), ('y', 'x')]
        actual_permutations = list(FlatZincInstanceGenerator.generate_permutations(variables))
        self.assertEqual(expected_permutations, actual_permutations)

    def test_substitute_variables(self):
        test_cases = [
            ('solve :: int_search([x, y],', ['a', 'b'], 'solve :: int_search([a,b],'),
            ('solve :: int_search([var1],', ['x'], 'solve :: int_search([x],'),
            ('solve :: int_search([],', [], 'solve :: int_search([],'),
            ('solve :: int_search([x, y, z],', ['a', 'b', 'c'], 'solve :: int_search([a,b,c],'),
            ('array [1..3] of var int: mark:: output_array([1..3]) = [1,2,3]; solve :: int_search(mark,', ['1', '2', '3'], 'array [1..3] of var int: mark:: output_array([1..3]) = [1,2,3]; solve :: int_search(mark,')
        ]

        for fzn_content, variables, expected_content in test_cases:
            with self.subTest(fzn_content=fzn_content, variables=variables):
                actual_content = FlatZincInstanceGenerator.substitute_variables(fzn_content, variables)
                self.assertEqual(expected_content, actual_content)



    def test_run(self):

        input_file = Path("test_data/feature_vector.parquet").resolve()
        temp_dir = tempfile.TemporaryDirectory()

        output_path = Path(temp_dir.name).resolve()
        generator = FlatZincInstanceGenerator(
            feature_vector_parquet_input_file=input_file,
            instances_parquet_output=output_path,
            max_vars=10
        )

        generator.run()
        self.assertTrue(output_path.exists(), "Output file was not created")

        # Load and verify some of the contents of the output file
        ds = pq.ParquetDataset(output_path, schema=Schemas.Parquet.instances)
        table = ds.read()

        col = table[Constants.INSTANCE_PERMUTATION]
        entry_length = len(col[0].as_py())

        for row in col:
            self.assertEqual(len(row.as_py()), entry_length, "Invalid Permutations")

        expected_row_count = math.factorial(entry_length)
        self.assertEqual(table.num_rows, expected_row_count, "Expected Permutation count differs")

        temp_dir.cleanup()


if __name__ == '__main__':
    unittest.main()
