import matplotlib.pyplot as plt
import pyarrow.parquet as pq
from collections import defaultdict
from schemas import Constants


if __name__ == "__main__":

    hists = [
        ("../temp/vectors_generic_cp.parquet", "histograms/combined_histograms_cp.svg", (10,6)),
        ("../temp/vectors_generic_gecode.parquet", "histograms/combined_histograms_gecode.svg", (10,6)),
        ("../temp/vectors_generic_api.parquet", "histograms/combined_histograms_api.svg", (2,6)),
    ]

    for hist in hists:

        parquet, savename, figsize = hist

        df = pq.read_table(parquet).to_pandas()
        histogram_data = defaultdict(int)

        for entry in df[Constants.CONSTRAINT_HISTOGRAM]:
            if isinstance(entry, list):
                for key, value in entry:
                    if isinstance(value, int):
                        histogram_data[key] += value

        histogram_data = sorted(histogram_data.items(), key=lambda x: x[1], reverse=True)
        keys = [k for k, v in histogram_data]
        values = [v for k, v in histogram_data]

        plt.figure(figsize=figsize)
        plt.bar(keys, values, color='skyblue')
        plt.xlabel('Constraints')
        plt.ylabel('Frequenz')
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(savename, format='svg')

        plt.show()