import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from schemas import Constants

# SHOW THE RELATION BETWEEN BACKTRACKS AND VARS VS BACKTRACKS AND CONSTRAINTS

instance_results = pd.read_parquet('../result.10000_vm')
failures_avg = instance_results.groupby(Constants.MODEL_NAME, observed=False)[Constants.FAILURES].mean().reset_index()

feature_vectors = pd.read_parquet('../temp/vector_big_10.parquet')
feature_vectors['totalVariables'] = feature_vectors[Constants.FLAT_INT_VARS] + feature_vectors[Constants.FLAT_SET_VARS] + feature_vectors[Constants.FLAT_BOOL_VARS]

df_combined = pd.merge(failures_avg, feature_vectors[[Constants.MODEL_NAME, Constants.TOTAL_CONSTRAINTS, 'totalVariables']], on=Constants.MODEL_NAME)
df_combined = df_combined[(df_combined['totalVariables'] > 0) & (df_combined[Constants.TOTAL_CONSTRAINTS] > 0) & (df_combined[Constants.FAILURES] > 0)]


plt.figure(figsize=(10,5))

plt.subplot(1,2,1)
plt.scatter(df_combined['totalVariables'], df_combined[Constants.FAILURES])
plt.xscale('log')
plt.yscale('log')
#plt.title('Backtracks (AVG) vs. Variablen [Log-Scale]')
plt.xlabel('Anzahl von Variablen [Log-Skala]')
plt.ylabel('Backtracks (AVG) [Log-Skala]')

#this is some very black magic
plt.plot(np.unique(df_combined['totalVariables']),
         np.poly1d(np.polyfit(np.log(df_combined['totalVariables']), np.log(df_combined[Constants.FAILURES]), 1))(np.unique(df_combined['totalVariables'])), color='red')

# Streudiagramm für Laufzeit (AVG Failures) vs. Anzahl der Constraints (log-Skalierung)
plt.subplot(1,2,2)
plt.scatter(df_combined[Constants.TOTAL_CONSTRAINTS], df_combined[Constants.FAILURES])
plt.xscale('log')
plt.yscale('log')
#plt.title('acktracks (AVG) vs. Constraints [Log-Scale]')
plt.xlabel('Anzahl von Constraints [Log-Skala]')
plt.ylabel('Backtracks (AVG) [Log-Skala]')

#this is some very black magic
plt.plot(np.unique(df_combined[Constants.TOTAL_CONSTRAINTS]),
         np.poly1d(np.polyfit(np.log(df_combined[Constants.TOTAL_CONSTRAINTS]), np.log(df_combined[Constants.FAILURES]), 1))(np.unique(df_combined[Constants.TOTAL_CONSTRAINTS])), color='red')

plt.savefig("scatterplots_versus.svg", format="svg")
plt.tight_layout()
plt.show()
