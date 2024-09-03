import math
import os
import unittest
import tempfile
from pathlib import Path

from instance_generator import FlatZincInstanceGenerator
from parquet import ParquetReader
from schemas import Schemas, Constants


class TestFlatZincInstanceGenerator(unittest.TestCase):

    def test_extract_variables(self):
        test_cases = [
            ('solve :: int_search([x, y],', (['x', 'y'], None)),
            ('solve :: int_search([23, b, c],', (['23', 'b', 'c'], None)),
            ('solve :: int_search([],', ([], None)),
            ('solve     ::      int_search([var1, var2, var3, var4],', (['var1', 'var2', 'var3', 'var4'], None)),
            ('array [1..6] of var int: mark:: output_array([1..6]) = [0,X_INTRODUCED_17_,X_INTRODUCED_18_,X_INTRODUCED_19_,X_INTRODUCED_20_,X_INTRODUCED_21_]; \n solve     ::      int_search(mark,', (['0','X_INTRODUCED_17_','X_INTRODUCED_18_','X_INTRODUCED_19_','X_INTRODUCED_20_','X_INTRODUCED_21_'], "mark"))
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
            ('solve :: int_search([x, y],', (['a', 'b'], None), 'solve :: int_search([a,b],'),
            ('solve :: int_search([var1],', (['x'], None), 'solve :: int_search([x],'),
            ('solve :: int_search([],', ([], None), 'solve :: int_search([],'),
            ('solve :: int_search([x, y, z],', (['a', 'b', 'c'], None), 'solve :: int_search([a,b,c],'),
            ('array [1..3] of var int: mark:: output_array([1..3]) = [1,2,3]; solve :: int_search(mark,', (['1', '2', '3'], 'mark'), 'array [1..3] of var int: mark:: output_array([1..3]) = [1,2,3]; solve :: int_search(mark,')
        ]

        for fzn_content, variables, expected_content in test_cases:
            with self.subTest(fzn_content=fzn_content, variables=variables):
                actual_content = FlatZincInstanceGenerator.substitute_variables(fzn_content, *variables)
                self.assertEqual(expected_content, actual_content)



    def test_run(self):

        input_file = "test_data/feature_vector.parquet"
        fd, filename = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)

        output_file = Path(filename).resolve()
        generator = FlatZincInstanceGenerator(
            feature_vector_parquet_input_file=Path(input_file).resolve(),
            instances_parquet_output_file=output_file,
            max_vars=10
        )

        generator.run()
        self.assertTrue( output_file.exists(), "Output file was not created")

        # Load and verify some of the contents of the output file
        reader = ParquetReader(Schemas.Parquet.instances,  Path(output_file).resolve())

        col = reader.table()[Constants.INSTANCE_PERMUTATION]
        entry_length = len(col[0].as_py())

        for row in col:
            self.assertEqual(len(row.as_py()), entry_length, "Invalid Permutations")

        expected_row_count = math.factorial(entry_length)
        self.assertEqual(reader.table().num_rows, expected_row_count, "Expected Permutation count differs")

        reader.release()
        Path(output_file).resolve().unlink()


if __name__ == '__main__':
    unittest.main()
