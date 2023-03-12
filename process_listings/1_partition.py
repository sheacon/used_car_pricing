import pandas as pd
import pyarrow
import os
import sys

input_dir = sys.argv[1]
file = sys.argv[2]
output_dir = sys.argv[3]

# debug
#input_dir = "/data/p_dsi/capstone_projects/shea/0_processed/full_data/"
#file = "part-00000-ab3867d4-3d1a-404d-aadb-d7f3afb6fd40-c000.snappy.parquet"
#output_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned_python/"

# read in the parquet file
df = pd.read_parquet(input_dir+file)

df = df.dropna(subset = ["scraped_at"])

df["scraped_at_year"] = pd.to_datetime(df['scraped_at'], unit='s').dt.year

# partition the data
partitions = df.groupby(["state", "scraped_at_year"])

# write each partition to its own directory
for (state, scraped_at_year), partition in partitions:
    partition_path = os.path.join(output_dir, state, str(scraped_at_year))
    os.makedirs(partition_path, exist_ok=True)
    partition.to_parquet(partition_path + "/" + file)
