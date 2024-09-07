from pathlib import Path

from feature_extraction import FeatureVectorExtractor
from instance_generator import FlatZincInstanceGenerator

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    input_files = [
        Path('temp/magic_sequence3.mzn').resolve(),               #as is
        Path('temp/golomb_rulers.mzn').resolve(),                 #as is (m = 6)
        Path('temp/template_design.mzn').resolve(),               #edited (all vars tuned down)
        Path('temp/langford2.mzn').resolve(),                     #as is
        Path('temp/queens5.mzn').resolve(),                       #as is
        Path('temp/K4xP2Graceful.mzn').resolve(),                 #as is
        Path('temp/diamond_free_degree_sequence.mzn').resolve(),  #edited (n 11 -> 10)
        Path('temp/rehearsal.mzn').resolve(),                     # mit choco.dzn (5) und smith.dzn (9)
        Path('temp/all_interval.mzn').resolve(),                  # edited (n 12 -> 10)
        Path('temp/traffic_lights.mzn').resolve(),                #as is

        #Path('temp/steiner.mzn').resolve(),                      #as is (uses set_search)
        #Path('temp/set_partition.mzn').resolve(),                 #as is (uses set_search)

    ]

    parquet_file = Path('temp/vector.parquet').resolve()
    extractor = FeatureVectorExtractor(input_files, parquet_file)
    extractor.run()

    #instances = Path('temp/instances.parquet').resolve()
    #instanceGen = FlatZincInstanceGenerator(parquet_file, instances, 10)
    #instanceGen.run()


