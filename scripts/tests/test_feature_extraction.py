import unittest
from pathlib import Path
from feature_extraction import FeatureVectorExtractor
import pyarrow.compute as pc
import pyarrow.parquet as pa
from unittest.mock import patch
from schemas import Schemas, Constants


class TestFeatureVectorExtractorWithRealFiles(unittest.TestCase):
    def setUp(self):
        self.input_files = [
            ("000", Path('test_data/test_model.mzn').resolve()),
            ("000", Path('test_data/test_model_2.mzn').resolve()),
            ("000", Path('test_data/test_model_3.mzn').resolve()),
            ("000", Path('test_data/golomb_rulers.mzn').resolve())
        ]
        self.parquet_file = Path('feature_vector_extraction_test.parquet').resolve()
        self.extractor = FeatureVectorExtractor(self.input_files, self.parquet_file)

    @patch.object(FeatureVectorExtractor, 'command_template', new=' --solver cp --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{mzn}" --json-stream --compile')
    def test_cp_feature_vector_extraction(self):
        # patch command template
        self.extractor.run()
        table = pa.read_table(schema=Schemas.Parquet.feature_vector, source=self.parquet_file)

        self.assertEqual(4, table.num_rows),
        self.assertEqual(Schemas.Parquet.feature_vector, table.schema),
        self.assertEqual(15, pc.mean(table[Constants.MEDIAN_DOMAIN_SIZE]).as_py()),
        self.assertEqual(0, pc.min(table[Constants.FLAT_INT_VARS]).as_py()),
        self.assertEqual(77, pc.max(table[Constants.FLAT_INT_VARS]).as_py()),
        self.assertEqual(4, pc.count(table[Constants.ANNOTATION_HISTOGRAM]).as_py()),
        self.assertAlmostEqual(8.016, pc.stddev(table[Constants.DOMAIN_WIDTHS][1].as_py()).as_py(), 2),

        print(table.schema)


    @patch.object(FeatureVectorExtractor, 'command_template', new=' --solver gecode --two-pass --feature-vector --no-output-ozn --output-fzn-to-file {fznfile} "{mzn}" --json-stream --compile')
    def test_gecode_feature_vector_extraction(self):
        # patch command template
        self.extractor.run()
        table = pa.read_table(schema=Schemas.Parquet.feature_vector, source=self.parquet_file)

        self.assertEqual(4, table.num_rows),
        self.assertEqual(Schemas.Parquet.feature_vector, table.schema),
        self.assertEqual(18.75, pc.mean(table[Constants.MEDIAN_DOMAIN_SIZE]).as_py()),
        self.assertEqual(0, pc.min(table[Constants.FLAT_INT_VARS]).as_py()),
        self.assertEqual(65, pc.max(table[Constants.FLAT_INT_VARS]).as_py()),
        self.assertEqual(4, pc.count(table[Constants.ANNOTATION_HISTOGRAM]).as_py()),
        self.assertAlmostEqual(8.48, pc.stddev(table[Constants.DOMAIN_WIDTHS][1].as_py()).as_py(),2),

        print(table.schema)

    def tearDown(self):
        if self.parquet_file.exists():
            self.parquet_file.unlink()
            pass

if __name__ == '__main__':
    unittest.main()