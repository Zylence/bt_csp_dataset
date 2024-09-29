import os
import pandas as pd
from PIL import Image
from schemas import Constants

def string_to_bitmap(bitstring, dpi=300):
    """
    Converts a Bitstring to Bitmap image
    """
    rows = bitstring.split('|')
    img_width = max(len(row) for row in rows)
    img_height = len(rows)

    img = Image.new('1', (img_width, img_height))
    pixels = img.load()

    for y, row in enumerate(rows):
        for x, bit in enumerate(row):
            pixels[x, y] = int(bit)

    # scale to keep 5x5 cm with dpi
    cm_to_inch = 2.54
    target_size_in_pixels = int((5 / cm_to_inch) * dpi)
    scaled_img = img.resize((target_size_in_pixels, target_size_in_pixels), Image.NEAREST)

    return scaled_img


parquet_file = "../temp/vector_big_6.parquet"
df = pd.read_parquet(parquet_file)

output_folder = "bitmaps_output"
os.makedirs(output_folder, exist_ok=True)

for index, row in df.iterrows():
    model_name = row[Constants.MODEL_NAME]
    constraint_graph = row[Constants.CONSTRAINT_GRAPH]  # Bitstring des Constraint-Graphs

    if isinstance(constraint_graph, str):
        bitmap = string_to_bitmap(constraint_graph)

        output_path = os.path.join(output_folder, f"{model_name}.png")
        bitmap.save(output_path, dpi=(300, 300))
        print(f"Saved: {output_path}")

print("All bitmaps have been generated and saved.")