"""

n = 119

Goal: Given optimal number of chunks, split data into those chunks by finding
the top-most coordinate, left-most coordinate, right-most coordinate, and
bottom-most coordinate for the entire dataset, and then use that bounding box
to divide the dataset into chunks by coordinate.

"""



import pandas as pd
import json
import os

try:
    output_file_path: str = './extra_python_files_etc/cached_data/chunked_well_data.json'
    with open(output_file_path, 'r') as output_file:
        grid_data = json.load(output_file)
    first_three_keys = list(grid_data.keys())[:3]
    print(f"First three keys in grid data: {first_three_keys}")
except FileNotFoundError:
    print(f"Grid data file not found at {output_file_path}")

# Define the file paths
json_file_path = './extra_python_files_etc/cached_data/condensed_well_data.json'
output_file_path = './extra_python_files_etc/cached_data/grid_well_data.json'
chunk_split_n = 119

# Load the data from the JSON file
if not os.path.exists(json_file_path):
    raise FileNotFoundError(f"JSON file not found at {json_file_path}")

with open(json_file_path, 'r') as json_file:
    well_data = json.load(json_file)

# Convert the dictionary to a DataFrame
df = pd.DataFrame(well_data)

# Find the left-most, top-most, right-most, and bottom-most coordinates
min_lat = df["lat"].min()
max_lat = df["lat"].max()
min_long = df["lon"].min()
max_long = df["lon"].max()

print(f"Bounding coordinates:")
print(f"  Top-left: ({min_lat}, {min_long})")
print(f"  Bottom-right: ({max_lat}, {max_long})")

# Calculate the grid steps
lat_step = (max_lat - min_lat) / chunk_split_n
long_step = (max_long - min_long) / chunk_split_n

# Initialize the grid dictionary
grid_data = {}

# Populate the grid with wells
for _, row in df.iterrows():
    lat = row["lat"]
    lon = row["lon"]
    
    # Determine the grid cell
    lat_index = int((lat - min_lat) / lat_step)
    long_index = int((lon - min_long) / long_step)
    
    # Correct index if it is on the boundary
    if lat_index == chunk_split_n:
        lat_index -= 1
    if long_index == chunk_split_n:
        long_index -= 1
    
    # Calculate the top-left and bottom-right coordinates of the grid cell
    top_left = (min_lat + lat_index * lat_step, min_long + long_index * long_step)
    bottom_right = (min_lat + (lat_index + 1) * lat_step, min_long + (long_index + 1) * long_step)
    cell_key = f"{top_left}-{bottom_right}"
    
    # Initialize the cell in the grid data if it doesn't exist
    if cell_key not in grid_data:
        grid_data[cell_key] = []
    
    # Add the well data to the grid cell
    well_info = row.to_dict()
    grid_data[cell_key].append(well_info)

# Save the grid data to a non-formatted JSON file
with open(output_file_path, 'w') as output_file:
    json.dump(grid_data, output_file, separators=(',', ':'))

print(f"Grid data saved to {output_file_path}")
