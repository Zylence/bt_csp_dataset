import os
import tempfile
import unittest
from pathlib import Path
from typing import Dict

import pandas as pd
from parquet import ParquetWriter, ParquetReader
from schemas import Constants, Schemas, Helpers


class TestSchemaAndTableUtils(unittest.TestCase):

    def setUp(self):
        with open("test_data/sample_feature_vector.json", 'r') as f:
            self.feature_vector_json = f.read()
        

        self.invalid_feature_vector_json = '''
        {
            "type": "feature_vector",
            "feature_vector": {
                "flatBoolVars": "invalid",  # Should be an integer
                "flatIntVars": 20
            }
        }
        '''

    def test_json_to_dataframe_valid(self):
        data = Helpers.json_to_normalized_feature_vector_dict(self.feature_vector_json)
        self.assertIsInstance(data, Dict)
        self.assertEqual(data[Constants.FLAT_BOOL_VARS], 13)
        self.assertEqual(data[Constants.FLAT_INT_VARS], 65)

    def test_json_to_dataframe_invalid(self):
        with self.assertRaises(ValueError):
            Helpers.parse_json_validated(self.invalid_feature_vector_json, Schemas.JSON.feature_vector)

    @staticmethod
    def assertSubset(tc: unittest.TestCase, subset: dict, actual: dict):
        for key, value in subset.items():
            tc.assertIn(key, actual, f"Key {key} not found in actual_dict")
            tc.assertEqual(actual[key], value, f"Value for key {key} does not match")

    def test_json_to_normalized_feature_vector_dataframe(self):
        data = Helpers.json_to_normalized_feature_vector_dict(self.feature_vector_json)
        self.assertIsInstance(data, Dict)
        TestSchemaAndTableUtils.assertSubset(self, {1: "X_INTRODUCED_17_", 2: "X_INTRODUCED_18_"}, data[Constants.ID_TO_VAR_NAME_MAP])
        TestSchemaAndTableUtils.assertSubset(self, {26: "int_lin_eq", 36: "bool2int"}, data[Constants.ID_TO_CONSTRAINT_NAME_MAP])

    def test_create_table(self):
        schema = Schemas.Parquet.feature_vector
        fd, temp_file = tempfile.mkstemp(suffix='.parquet')
        os.close(fd)

        writer = ParquetWriter(schema, Path(temp_file).resolve(), overwrite=True)
        writer.close_table()
        reader = ParquetReader(schema, Path(temp_file).resolve())

        self.assertEqual(reader.table.schema, schema)
        self.assertEqual(reader.table.num_rows, 0)

        os.remove(temp_file)

    def test_append_row(self):
        schema = Schemas.Parquet.feature_vector
        writer = ParquetWriter(schema)

        data = Helpers.json_to_normalized_feature_vector_dict(self.feature_vector_json)
        data[Constants.PROBLEM_ID] = "somestring"
        writer.append_row(data)

        self.assertEqual(len(writer.data), 1)
        self.assertEqual(set(writer.data[0].keys()), set(schema.names))

    def test_save_table(self):
        schema = Schemas.Parquet.feature_vector
        writer = ParquetWriter(schema)

        data = Helpers.json_to_normalized_feature_vector_dict(self.feature_vector_json)
        data[Constants.PROBLEM_ID] = "somestring"
        writer.append_row(data)

        writer.close_table('test.parquet')

        reader = ParquetReader(schema)
        reader.load('test.parquet')

        self.assertEqual(reader.table.num_rows, 1)
        self.assertEqual(reader.table.schema, schema)


if __name__ == '__main__':
    unittest.main()
