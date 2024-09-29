import matplotlib.pyplot as plt
import pandas as pd

from schemas import Constants

instance_results = pd.read_parquet('../result')

failures_extremes = instance_results.groupby(Constants.MODEL_NAME, observed=False)[Constants.FAILURES].agg(['max', 'min']).reset_index()
failures_extremes['Difference'] = failures_extremes['max'] - failures_extremes['min']

model_min_diff = failures_extremes.loc[failures_extremes['Difference'].idxmin(), Constants.MODEL_NAME]
model_data_min = instance_results[instance_results[Constants.MODEL_NAME] == model_min_diff]
model_data_min = model_data_min.sort_values(by=Constants.ID)

plt.figure(figsize=(10, 5))
plt.plot(model_data_min[Constants.ID], model_data_min[Constants.FAILURES], alpha=0.7)  # TODO: ID später durch PERM_INDEX ersetzen
plt.title(f'Datenpunkte für {model_min_diff} (Wenigster Unterschied in Failures)')
plt.xlabel('Index')
plt.ylabel('Backtracks')

# max and min config
max_failures_min = model_data_min[Constants.FAILURES].max()
min_failures_min = model_data_min[Constants.FAILURES].min()
plt.axhline(y=max_failures_min, color='green', linestyle='--', label='Max Failures')
plt.axhline(y=min_failures_min, color='orange', linestyle='--', label='Min Failures')
plt.legend()
plt.show()
