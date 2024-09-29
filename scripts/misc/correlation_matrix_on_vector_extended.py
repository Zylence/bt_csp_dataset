import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from schemas import Constants

file_path = '../temp/vector_big_6.parquet'
df = pd.read_parquet(file_path)

df[Constants.DOMAIN_WIDTHS] = df[Constants.DOMAIN_WIDTHS].apply(sum)
df[Constants.CONSTRAINT_HISTOGRAM] = df[Constants.CONSTRAINT_HISTOGRAM].apply(lambda x: sum([z for _, z in x]))
df[Constants.ANNOTATION_HISTOGRAM] = df[Constants.ANNOTATION_HISTOGRAM].apply(lambda x: sum([z for _, z in x]))

correlation_data = df[[Constants.FLAT_BOOL_VARS,
                        Constants.FLAT_INT_VARS,
                        Constants.FLAT_SET_VARS,
                        Constants.DOMAIN_WIDTHS,
                        Constants.STD_DEVIATION_DOMAIN,
                        Constants.AVERAGE_DOMAIN_SIZE,
                        Constants.MEDIAN_DOMAIN_SIZE,
                        Constants.AVERAGE_DOMAIN_OVERLAP,
                        Constants.NUMBER_OF_DISJOINT_PAIRS,
                        Constants.META_CONSTRAINTS,
                        Constants.TOTAL_CONSTRAINTS,
                        Constants.CONSTRAINT_HISTOGRAM,
                        Constants.ANNOTATION_HISTOGRAM,
                        Constants.AVG_DECISION_VARS_IN_CONSTRAINTS]]

correlation_matrix = correlation_data.corr()

plt.figure(figsize=(20, 20))
sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm', square=True, cbar_kws={"shrink": .8})
plt.savefig('correlation_matrix_extended.svg', format='svg')
plt.show()
