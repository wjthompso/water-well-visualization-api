import pandas as pd
import json
import math
from collections import defaultdict
import time
import os

# Define the file paths
csv_file_path = "/Users/wthompson/Downloads/2024_03_05_LithologicLogData/AAA_Hydrolithology.csv"
json_file_path = './extra_python_files_etc/cached_data/condensed_well_data.json'

# Define the metadata fields
metadata_fields = [
    "TopDepth_InUnitsOfFeet", "BottomDepth_InUnitsOfFeet", "String", "Category", 
    "SpecificRetention", "SpecificYield", "Porosity", 
    "SpecificRetentionDividedByPorosity"
]

# Define the category mappings
category_mappings = {
    "short_to_long_map": {
        "CARB": "12 Mostly carbonate rock (e.g., limestone, dolomite)",
        "CFGP": "11 Consolidated: Fine grained low-permeability (e.g., siltstone, shale, chert)",
        "UMIX": "3 Unconsolidated: Mixture of coarse and fine grained (e.g., undifferentiated soil)",
        "UCG": "1 Unconsolidated: Coarse-grained material(e.g., sand, gravel)",
        "UFG": "5 Unconsolidated: Fine grained material (e.g., silt, clay)",
        "CCG": "7 Consolidated: Coarse-grained material(e.g., sandstone, conglomerate)",
        "UMC": "2 Unconsolidated: Mostly coarse-grained (e.g., sand, gravel) some fine-grained material",
        "UFC": "4 Unconsolidated: Mostly fine-grained (e.g., silt, clay) some coarse-grained material",
        "EVAP": "15 Evaporites (e.g., gypsum)",
        "ENDO": "13 Endogenous (metamorphic rocks, granite, etc.)",
        "CMIX": "9 Consolidated: Mixture of coarse and fine grained",
        "CMFC": "10 Consolidated: Mostly fine-grained low-permeability (e.g., siltstone, shale, chert) some coarse grained material",
        "CMCG": "8 Consolidated: Mostly coarse-grained (e.g., sandstone, conglomerate) some fine grained material",
        "VOLC": "14 Volcanic (rhyolite, basalt)",
        "TILL": "6 Till/drift"
    },
    "long_to_short_map": {
        "12 Mostly carbonate rock (e.g., limestone, dolomite)": "CARB",
        "11 Consolidated: Fine grained low-permeability (e.g., siltstone, shale, chert)": "CFGP",
        "3 Unconsolidated: Mixture of coarse and fine grained (e.g., undifferentiated soil)": "UMIX",
        "1 Unconsolidated: Coarse-grained material(e.g., sand, gravel)": "UCG",
        "5 Unconsolidated: Fine grained material (e.g., silt, clay)": "UFG",
        "7 Consolidated: Coarse-grained material(e.g., sandstone, conglomerate)": "CCG",
        "2 Unconsolidated: Mostly coarse-grained (e.g., sand, gravel) some fine-grained material": "UMC",
        "4 Unconsolidated: Mostly fine-grained (e.g., silt, clay) some coarse-grained material": "UFC",
        "15 Evaporites (e.g., gypsum)": "EVAP",
        "13 Endogenous (metamorphic rocks, granite, etc.)": "ENDO",
        "9 Consolidated: Mixture of coarse and fine grained": "CMIX",
        "10 Consolidated: Mostly fine-grained low-permeability (e.g., siltstone, shale, chert) some coarse grained material": "CMFC",
        "8 Consolidated: Mostly coarse-grained (e.g., sandstone, conglomerate) some fine grained material": "CMCG",
        "14 Volcanic (rhyolite, basalt)": "VOLC",
        "6 Till/drift": "TILL"
    }
}

# Check if the JSON file exists
if os.path.exists(json_file_path):
    # Load the data from the JSON file
    with open(json_file_path, 'r') as json_file:
        well_data = json.load(json_file)
    # Convert the dictionary to a DataFrame
    grouped = pd.DataFrame(list(well_data))
    print("Loaded data from JSON file.")
else:
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Use a dictionary to accumulate data by WellID
    well_data = defaultdict(lambda: {"layers": []})
    total_rows = len(df)
    print_interval = total_rows // 10  # Print progress every 10% of the data
    start_time = time.time()

    for i, row in enumerate(df.itertuples(index=False), start=1):
        well_id = row.WellID
        if "well_id" not in well_data[well_id]:
            well_data[well_id].update({
                "well_id": well_id,
                "total_well_depth_in_ft": row.WellDepth_InUnitsOfFeet,
                "lat": row.Lat,
                "lon": row.Lon
            })
        layer = tuple(getattr(row, field) if field != "Category" else category_mappings["long_to_short_map"].get(getattr(row, field), getattr(row, field)) for field in metadata_fields)
        well_data[well_id]["layers"].append(layer)

        # Print progress
        if i % print_interval == 0:
            elapsed_time = time.time() - start_time
            print(f"Processed {i} / {total_rows} rows ({(i / total_rows) * 100:.2f}%) in {elapsed_time:.2f} seconds")

    # Convert the dictionary to a DataFrame
    grouped = pd.DataFrame(list(well_data.values()))

    # Save transformed data to JSON in a non-formatted format
    # Convert the dictionary to a DataFrame
    grouped = pd.DataFrame(list(well_data.values()))

    # Manually handle JSON serialization to ensure compact format
    with open(json_file_path, 'w') as output_file:
        json.dump(grouped.to_dict(orient='records'), output_file, separators=(',', ':'))
    print("Saved condensed data to JSON file.")

# Find the bounding box
min_lat = grouped["lat"].min()
max_lat = grouped["lat"].max()
min_long = grouped["lon"].min()
max_long = grouped["lon"].max()

print(f"{min_lat}, {max_lat}, {min_long}, {max_long}")
print(f"Top left: {min_lat}, {min_long}")
print(f"Top right: {min_lat}, {max_long}")

# Calculate the average size of each JSON entry in bytes
json_sizes = grouped.apply(lambda x: len(json.dumps(x.to_dict(), separators=(',', ':')).encode('utf-8')), axis=1)
average_size_in_bytes_of_entry = json_sizes.mean()
average_size_in_mb = average_size_in_bytes_of_entry / 1024 / 1024
print(f"Average size in bytes of each entry: {average_size_in_bytes_of_entry}")
print(f"Average size in megabytes of each entry: {average_size_in_mb}")

# Define target chunk sizes
target_avg_chunk_size_mb = 6
max_chunk_size_mb = 14
target_avg_chunk_size_bytes = target_avg_chunk_size_mb * 1024 * 1024
max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024

# Start with n = 1
n = 1
largest_chunk_size = float('inf')
average_chunk_size = float('inf')

while largest_chunk_size > max_chunk_size_bytes or average_chunk_size > target_avg_chunk_size_bytes:
    # Calculate the grid steps
    lat_step = (max_lat - min_lat) / (n + 1)
    long_step = (max_long - min_long) / (n + 1)
    
    sub_squares = []
    largest_chunk_size = 0
    total_chunk_size = 0
    
    # Create the grid
    lat_bins = pd.cut(grouped["lat"], bins=n+1, labels=False)
    long_bins = pd.cut(grouped["lon"], bins=n+1, labels=False)
    
    # Combine latitude and longitude bins to create grid cells
    grid = grouped.groupby([lat_bins, long_bins]).size()
    
    # Calculate sizes
    for size in grid:
        sub_square_size = size * average_size_in_bytes_of_entry
        sub_squares.append(sub_square_size)
        total_chunk_size += sub_square_size
        if sub_square_size > largest_chunk_size:
            largest_chunk_size = sub_square_size
    
    average_chunk_size = total_chunk_size / len(sub_squares)
    average_chunk_size_mb = average_chunk_size / (1024 * 1024)
    largest_chunk_size_mb = largest_chunk_size / (1024 * 1024)
    
    print(f"Iteration {n}:")
    print(f"  Average chunk size in MB: {average_chunk_size_mb}")
    print(f"  Largest chunk size in MB: {largest_chunk_size_mb}")
    
    n += 1

print(f"Final grid size: {n - 1}x{n - 1}")
print(f"Average number of well entry rows within each sub-square: {average_chunk_size / average_size_in_bytes_of_entry}")
print(f"Average chunk size in MB: {average_chunk_size_mb}")
print(f"Largest chunk size in bytes: {largest_chunk_size}")
print(f"Largest chunk size in MB: {largest_chunk_size_mb}")
