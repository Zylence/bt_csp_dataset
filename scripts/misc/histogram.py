import matplotlib.pyplot as plt
import pyarrow.parquet as pq
from collections import defaultdict
from schemas import Constants

df = pq.read_table('../temp/vector_big_7.parquet').to_pandas()
histogram_data = defaultdict(int)

for entry in df[Constants.CONSTRAINT_HISTOGRAM]:
    if isinstance(entry, list):
        for key, value in entry:
            if isinstance(value, int):
                histogram_data[key] += value

histogram_data = sorted(histogram_data.items(), key=lambda x: x[1], reverse=True)
keys = [k for k, v in histogram_data]
values = [v for k, v in histogram_data]

plt.figure(figsize=(10, 6))
plt.bar(keys, values, color='skyblue')
plt.xlabel('Constraints')
plt.ylabel('Frequenz')
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig('aggregated_constraint_histogram.svg', format='svg')

plt.show()