import os
import pandas as pd
from PIL import Image
import svgwrite
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

def save_as_svg(bitmap, output_path):
    """Converts bitmap to (efficient) svg"""
    bitmap = bitmap.convert("1")
    width, height = bitmap.size
    dwg = svgwrite.Drawing(output_path)

    # List to accumulate path data for filled areas
    path_data = []
    start_x = None

    for y in range(height):
        for x in range(width):
            pixel = bitmap.getpixel((x, y))
            if pixel == 0:  # Black pixel
                if start_x is None:
                    start_x = x  # Start of a new filled area
            else:
                if start_x is not None:
                    # End of a filled area
                    path_data.append(f"M{start_x},{y}h{(x - start_x)}v1h-{(x - start_x)}z")
                    start_x = None

        # Unclosed filled area at the end of the row
        if start_x is not None:
            path_data.append(f"M{start_x},{y}h{(width - start_x)}v1h-{(width - start_x)}z")
            start_x = None

    # Combine all paths into a single path element
    if path_data:
        dwg.add(dwg.path(d=" ".join(path_data), fill='black'))

    dwg.save()

parquet_file = "../temp/vector_big_10.parquet"
df = pd.read_parquet(parquet_file)

output_folder = "bitmaps_output"
os.makedirs(output_folder, exist_ok=True)

for index, row in df.iterrows():
    model_name = row[Constants.MODEL_NAME]
    constraint_graph = row[Constants.CONSTRAINT_GRAPH]  # Bitstring des Constraint-Graphs

    if isinstance(constraint_graph, str):
        bitmap = string_to_bitmap(constraint_graph)

        output_path = os.path.join(output_folder, f"{model_name}.svg")
        #bitmap.save(output_path, dpi=(300, 300))
        save_as_svg(bitmap, output_path)
        print(f"Saved: {output_path}")

print("All bitmaps have been generated and saved.")