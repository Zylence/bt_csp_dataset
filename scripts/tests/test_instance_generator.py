import math
import os
import unittest
import tempfile
from pathlib import Path

from instance_generator import FlatZincInstanceGenerator
from parquet import ParquetReader
from schemas import Schemas, Constants


class TestFlatZincInstanceGenerator(unittest.TestCase):

    def setUp(self):
        # Create temporary files for input and output
        self.input_file = "test_data/feature_vector.parquet"
        fd, filename = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)

        self.output_file = filename
        # Initialize the FlatZincInstanceGenerator with temporary files
        self.generator = FlatZincInstanceGenerator(
            feature_vector_parquet_input_file=Path(self.input_file).resolve(),
            instances_parquet_output_file=Path(self.output_file).resolve()
        )

    def tearDown(self):
        Path(self.output_file).unlink()

    def test_extract_variables(self):
        test_cases = [
            ('solve :: int_search([x, y],', ['x', 'y']),
            ('solve :: int_search([23, b, c],', ['23', 'b', 'c']),
            ('solve :: int_search([],', []),
            ('solve     ::      int_search([var1, var2, var3, var4],', ['var1', 'var2', 'var3', 'var4'])
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
            ('solve :: int_search([x, y, z],', ['a', 'b', 'c'], 'solve :: int_search([a,b,c],')
        ]

        for fzn_content, variables, expected_content in test_cases:
            with self.subTest(fzn_content=fzn_content, variables=variables):
                actual_content = FlatZincInstanceGenerator.substitute_variables(fzn_content, variables)
                self.assertEqual(expected_content, actual_content)

    def test_run(self):
        self.generator.run()
        self.assertTrue(Path(self.output_file).exists(), "Output file was not created")

        # Load and verify some of the contents of the output file
        reader = ParquetReader(Schemas.Parquet.instances)
        reader.load_table(self.output_file)

        col = reader.table[Constants.INSTANCE_PERMUTATION]
        entry_length = len(col[0].as_py())

        for row in col:
            self.assertEqual(len(row.as_py()), entry_length, "Invalid Permutations")

        expected_row_count = math.factorial(entry_length)
        self.assertEqual(reader.table.num_rows, expected_row_count, "Expected Permutation count differs")


if __name__ == '__main__':
    unittest.main()
