import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

file_path = '../temp/vector_big_5.parquet'
df = pd.read_parquet(file_path)

numeric_df = df.select_dtypes(include=['number'])
correlation_matrix = numeric_df.corr()

plt.figure(figsize=(20, 20))
sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm', square=True, cbar_kws={"shrink": .8})
plt.savefig('correlation_matrix.svg', format='svg')
plt.show()
