import matplotlib.pyplot as plt
import pandas as pd

from schemas import Constants

instance_results = pd.read_parquet('../result.10000_vm')

failures_extremes = instance_results.groupby(Constants.MODEL_NAME, observed=False)[Constants.FAILURES].agg(['max', 'min']).reset_index()
failures_extremes['Difference'] = failures_extremes['max'] - failures_extremes['min']

model_max_diff = failures_extremes.loc[failures_extremes['Difference'].idxmax(), Constants.MODEL_NAME]
model_data = instance_results[instance_results[Constants.MODEL_NAME] == model_max_diff]
model_data = model_data.sort_values(by=Constants.ID)

plt.figure(figsize=(10, 5))
plt.plot(model_data[Constants.ID], model_data[Constants.FAILURES], alpha=0.7)  # TODO: ID später durch PERM_INDEX ersetzen
plt.xlabel('Lösungs ID (Nicht gleichbedeutend mit "permutationId")')
plt.ylabel('Backtracks')

# max and min data
max_failures = model_data[Constants.FAILURES].max()
min_failures = model_data[Constants.FAILURES].min()
max_index = model_data[model_data[Constants.FAILURES] == max_failures][Constants.ID].values[0]
min_index = model_data[model_data[Constants.FAILURES] == min_failures][Constants.ID].values[0]

# Max and Min config
plt.axhline(y=max_failures, color='green', linestyle='--', label='Max Backtracks')
plt.axhline(y=min_failures, color='orange', linestyle='--', label='Min Backtracks')
plt.scatter(max_index, max_failures, color='green', s=100, zorder=5)
plt.scatter(min_index, min_failures, color='orange', s=100, zorder=5)
plt.text(max_index, max_failures, f'Backtracks: {max_failures}\nID: {max_index}',
         fontsize=10, verticalalignment='bottom', horizontalalignment='left')
plt.text(min_index, min_failures + 2, f'Backtracks: {min_failures}\nID: {min_index}',  # Adjusted Y position for Min
         fontsize=10, verticalalignment='bottom', horizontalalignment='right')  # Move up by adding an offset

plt.legend()
plt.savefig("min_max/max_diff.svg", format="svg")
plt.show()
