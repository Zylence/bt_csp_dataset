import argparse
from pathlib import Path

from feature_extraction import FeatureVectorExtractor
from instance_generator import FlatZincInstanceGenerator
from testdriver import Testdriver

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CMDline Utility for feature extraction, instance generation and testing of csplib instances.')
    subparsers = parser.add_subparsers(dest='command')
    # FlatZincInstanceGenerator command with short options
    parser_fz = subparsers.add_parser('generate', aliases=['-g'], help='Generate FlatZinc instances')
    parser_fz.add_argument('-f', '--feature_vector_parquet_input_file', type=Path, required=True,
                           help='Input Parquet file for feature vectors')
    parser_fz.add_argument('-o', '--instances_parquet_output', type=Path, required=True,
                           help='Output folder for instances Parquet files')
    parser_fz.add_argument('-t', '--max_perms', type=int, required=True, help='Maximum number of permutations to compute per problem')
    parser_fz.add_argument('-c', '--cutoff_excess', action='store_true', help='Cut off excess variables', default=False)

    # Testdriver command with short options
    parser_td = subparsers.add_parser('test', aliases=['-t'], help='Run the test driver')
    parser_td.add_argument('-f', '--feature_vector_parquet', type=Path, required=True,
                           help='Feature vector Parquet file')
    parser_td.add_argument('-w', '--workload_parquet_folder', type=Path, required=True,
                           help='Folder for workload Parquet files')
    parser_td.add_argument('-o', '--output_folder', type=Path, required=True, help='Output folder for results')
    parser_td.add_argument('-l', '--log_path', type=Path, required=True, help='Log file path')
    parser_td.add_argument('-b', '--backup_path', type=Path, required=True, help='Backup file path')

    # FeatureVectorExtractor command with short options
    parser_fve = subparsers.add_parser('extract', aliases=['-e'], help='Extract feature vectors')
    parser_fve.add_argument('-i', '--input_files', type=Path, required=True,
                            help='Input files for feature extraction')
    parser_fve.add_argument('-o', '--parquet_output_file', type=Path, required=True,
                            help='Output Parquet file for feature vectors')

    args = parser.parse_args()

    #args.input_files = [
    #    ("019", Path('temp/magic_sequence3.mzn').resolve()),               #as is
    #    ("006", Path('temp/golomb_rulers.mzn').resolve()),                 #as is (m = 6)
    #    ("002", Path('temp/template_design.mzn').resolve()),               #edited (all vars tuned down)
    #    ("024", Path('temp/langford2.mzn').resolve()),                     #as is
    #    ("054", Path('temp/queens5.mzn').resolve()),                       #as is
    #    ("053", Path('temp/K4xP2Graceful.mzn').resolve()),                 #as is
    #    ("050", Path('temp/diamond_free_degree_sequence.mzn').resolve()),  #edited (n 11 -> 10)
    #    ("039", Path('temp/rehearsal.mzn').resolve()),                     # mit choco.dzn (5) und smith.dzn (9)
    #    ("007", Path('temp/all_interval.mzn').resolve()),                  # edited (n 12 -> 10)
    #    ("016", Path('temp/traffic_lights.mzn').resolve()),                #as is

    # Path('temp/steiner.mzn').resolve(),                      #as is (uses set_search)
    # Path('temp/set_partition.mzn').resolve(),                 #as is (uses set_search)
    # ]

    if args.command in ['generate', '-g']:
        generator = FlatZincInstanceGenerator(
            feature_vector_parquet_input_file=args.feature_vector_parquet_input_file,
            instances_parquet_output=args.instances_parquet_output,
            max_perms=args.targeted_vars,
            cutoff_excess=args.cutoff_excess
        )
        generator.run()

    elif args.command in ['test', '-t']:
        test_driver = Testdriver(
            feature_vector_parquet=args.feature_vector_parquet,
            workload_parquet_folder=args.workload_parquet_folder,
            output_folder=args.output_folder,
            log_path=args.log_path,
            backup_path=args.backup_path
        )
        test_driver.run()

    elif args.command in ['extract', '-e']:
        extractor = FeatureVectorExtractor(
            input_files=FeatureVectorExtractor.input_format_helper(args.input_files),
            parquet_output_file=args.parquet_output_file
        )
        extractor.run()


