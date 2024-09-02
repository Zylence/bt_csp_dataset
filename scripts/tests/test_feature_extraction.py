import unittest
from pathlib import Path
from feature_extraction import FeatureVectorExtractor
import pyarrow.compute as pc

from schemas import Schemas, Constants


class TestFeatureVectorExtractorWithRealFiles(unittest.TestCase):
    def setUp(self):
        self.input_files = [
            Path('test_data/test_model.mzn').resolve(),
            Path('test_data/test_model_2.mzn').resolve(),
            Path('test_data/test_model_3.mzn').resolve(),
            Path('test_data/golomb_rulers.mzn').resolve()
        ]
        self.parquet_file = Path('feature_vector_extraction_test.parquet').resolve()
        self.extractor = FeatureVectorExtractor(self.input_files, self.parquet_file)

    def test_feature_vector_extraction(self):
        self.extractor.run()

        res = self.extractor.writer.synced_access(
            lambda table: (
                # some sanity checks on the parsed files
                self.assertEqual(table.num_rows, 4),
                self.assertEqual(table.schema, Schemas.Parquet.feature_vector),
                self.assertEqual(pc.mean(table[Constants.MEDIAN_DOMAIN_SIZE]).as_py(), 18.75),  # or 15 with no optimization
                self.assertEqual(pc.min(table[Constants.FLAT_INT_VARS]).as_py(), 0),
                self.assertEqual(pc.max(table[Constants.FLAT_INT_VARS]).as_py(), 65),
                self.assertEqual(pc.count(table[Constants.ANNOTATION_HISTOGRAM]).as_py(), 4),
                self.assertAlmostEqual(pc.stddev(table[Constants.DOMAIN_WIDTHS][1].as_py()).as_py(), 8.48, 2),
                table.schema
            )
        )

        print(res[7])

    def tearDown(self):
        if self.parquet_file.exists():
            self.parquet_file.unlink()
            pass

if __name__ == '__main__':
    unittest.main()