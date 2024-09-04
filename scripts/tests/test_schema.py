import os
import tempfile
import unittest
from pathlib import Path
from typing import Dict

import pyarrow.parquet as pq
import pyarrow as pa
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

    def test_write_table(self):
        schema = Schemas.Parquet.feature_vector
        fd, temp_file = tempfile.mkstemp(suffix='.parquet')
        os.close(fd)

        writer = pq.ParquetWriter(Path(temp_file).resolve(), schema=schema)
        data = Helpers.json_to_normalized_feature_vector_dict(self.feature_vector_json)
        data[Constants.PROBLEM_ID] = "somestring"
        table = pa.Table.from_pylist([data], schema=schema)
        writer.write_table(table)
        writer.close()

        res_table = pq.read_table(Path(temp_file).resolve(), schema=schema)

        self.assertEqual(res_table.num_rows, 1)
        self.assertEqual(set(res_table.schema.names), set(schema.names))

        os.remove(temp_file)



if __name__ == '__main__':
    unittest.main()
