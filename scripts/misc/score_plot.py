import matplotlib.pyplot as plt
import numpy as np

# File formats and their properties
formats = ['CSV', 'Feather', 'Parquet', 'HDF5']
ram_usage = [0, 2, 2, 2]
schemas = [0, 0, 1, 0]
read_speed = [1, 2, 2, 1]
write_speed = [1, 2,2, 1]
datatype_avail = [0, 1, 2, 2]
compression = [0, 1, 1, 1]
platform_indep = [1, 1, 1, 1]

scores = []
for i in range(len(formats)):
    total_score = 0
    total_score += ram_usage[i]
    total_score += schemas[i]
    total_score += read_speed[i]
    total_score += write_speed[i]
    total_score += datatype_avail[i]
    total_score += compression[i]
    total_score += platform_indep[i]
    scores.append(total_score)


combined = list(zip(formats, scores))
combined.sort(key=lambda x: x[1], reverse=True)
sorted_formats, sorted_scores = zip(*combined)

fig, ax = plt.subplots(figsize=(7, 3))
y_pos = np.arange(len(sorted_formats))
ax.barh(y_pos, sorted_scores, align='center', color='skyblue')
ax.set_yticks(y_pos)
ax.set_yticklabels(sorted_formats)
ax.invert_yaxis()  # Invert the y-axis to have the highest score on top
ax.set_xlabel('Punkte')

plt.tight_layout()
plt.savefig('score_diagram.svg', format='svg')

plt.show()