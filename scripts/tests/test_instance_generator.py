import itertools
import math
import unittest
import tempfile
import random
from pathlib import Path

import pyarrow.parquet as pq
from instance_generator import FlatZincInstanceGenerator
from schemas import Schemas, Constants


class TestFlatZincInstanceGenerator(unittest.TestCase):

    def setUp(self):
        input_file = Path("test_data/feature_vector.parquet").resolve()
        self.temp_dir = tempfile.TemporaryDirectory()

        self.output_path = Path(self.temp_dir.name).resolve()
        self.generator = FlatZincInstanceGenerator(
            feature_vector_parquet_input_file=input_file,
            instances_parquet_output=self.output_path,
            max_vars=10
        )

    def tearDown(self):
        self.temp_dir.cleanup()

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
        expected_permutations = [['x', 'y'], ['y', 'x']]

        actual_permutations = self.generator.generate_permutations(variables)
        self.assertEqual(expected_permutations, actual_permutations)


    @staticmethod
    def nt_permutation_itertools_helper(variables, max_vars) -> list[list]:
        """We can not use this directly, as this is not memory efficient and way slower as it needs to generate all variables"""
        potential_perm_count = math.factorial(len(variables))
        num_computable_perms = min(max_vars, potential_perm_count)
        stepsize = potential_perm_count // num_computable_perms
        return [v for i, v in enumerate(map(list, itertools.permutations(variables))) if i % stepsize == 0]

    def test_permutation_generation_against_itertools(self):
        """
        Testing instance generation against itertools result.
        We can not use lists larger than 10 here (because of itertools)
        """
        test_cases = [
            (list(range(0, 10, 1)), math.factorial(10)),                      # continuous and large
            (list(range(0, 8, 1)), 13333),                                    # gap series
            (list(range(2, 6, 4)), math.factorial(6)),                        # continuous with offset
            ([str(i) for i in range(3, 7)], math.factorial(7)),               # continuous other dtype and offset
            ([str(i) for i in range(0, 18, 2)], 200),                         # continuous other dtype
            (list(range(8, 1, 1)), 1),                                        # invalid, should be empty
            (list(range(random.randrange(0, 10),
                        random.randrange(0,10),
                        random.randrange(0,10))),
             math.factorial(random.randrange(1,10)))               # everything randomized
        ]

        for variables, max_vars in test_cases:
            expected_permutations = TestFlatZincInstanceGenerator.nt_permutation_itertools_helper(variables, max_vars)
            with self.subTest(variables=variables):
                self.generator.max_vars = max_vars
                actual_vars = self.generator.generate_permutations(variables)
                for av, ev in zip(actual_vars, expected_permutations):
                    self.assertEqual(av, ev)

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

        self.generator.run()
        self.assertTrue(self.output_path.exists(), "Output file was not created")

        # Load and verify some of the contents of the output file
        ds = pq.ParquetDataset(self.output_path, schema=Schemas.Parquet.instances)
        table = ds.read()

        col = table[Constants.INSTANCE_PERMUTATION]
        entry_length = len(col[0].as_py())

        for row in col:
            self.assertEqual(len(row.as_py()), entry_length, "Invalid Permutations")

        expected_row_count = math.factorial(entry_length)
        self.assertEqual(table.num_rows, expected_row_count, "Expected Permutation count differs")

if __name__ == '__main__':
    unittest.main()
