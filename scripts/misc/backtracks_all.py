import os
import matplotlib.pyplot as plt
import pandas as pd

from schemas import Constants

instance_results = pd.read_parquet('../result')
output_dir = 'backtracks'
os.makedirs(output_dir, exist_ok=True)

failures_extremes = instance_results.groupby(Constants.MODEL_NAME, observed=False)[Constants.FAILURES].agg(
    ['max', 'min']).reset_index()

# Generates a plot for every model. technically makes max and min diff obsolete
for _, row in failures_extremes.iterrows():
    model_name = row[Constants.MODEL_NAME]
    model_data = instance_results[instance_results[Constants.MODEL_NAME] == model_name].sort_values(by=Constants.ID)

    # Plot the data points (failures) for each model
    plt.figure(figsize=(10, 5))
    plt.plot(model_data[Constants.ID], model_data[Constants.FAILURES], alpha=0.7, marker='o')
    plt.xlabel('Index')  # TODO: Replace with PERM_INDEX later
    plt.ylabel('Backtracks')

    # min and max data
    max_failures = model_data[Constants.FAILURES].max()
    min_failures = model_data[Constants.FAILURES].min()
    max_index = model_data[model_data[Constants.FAILURES] == max_failures][Constants.ID].values[0]
    min_index = model_data[model_data[Constants.FAILURES] == min_failures][Constants.ID].values[0]

    # min and max config
    plt.axhline(y=max_failures, color='green', linestyle='--', label='Max Failures')
    plt.axhline(y=min_failures, color='orange', linestyle='--', label='Min Failures')
    plt.scatter(max_index, max_failures, color='green')
    plt.scatter(min_index, min_failures, color='orange')
    plt.text(max_index, max_failures + 1, f'{max_failures}', color='green', ha='center')
    plt.text(min_index, min_failures + 1, f'{min_failures}', color='orange', ha='center')

    plt.legend()
    #plt.title(f'Datenpunkte für {model_name} (Höchster Unterschied in Failures)')
    plt.tight_layout()

    # Save the figure to the specified directory
    plt.savefig(os.path.join(output_dir, f'{model_name}_failures_plot.png')) # todo if we use any, make svg
    plt.close()  # Close the figure to free memory

print(f'Plots have been saved to the "{output_dir}" directory.')
