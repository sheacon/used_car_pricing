import pandas as pd
import pyarrow
import os
import sys
from glob import glob

input_dir = sys.argv[1]
file_section = int(sys.argv[2])
output_dir = sys.argv[3]

# debug
#input_dir = "/data/p_dsi/capstone_projects/shea/0_processed/full_data/"
#file_section = 2
#output_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned_array/"

file_list = glob(input_dir + '*.parquet')
files_per_section = len(file_list)//99
file_sublist = file_list[(file_section-1)*files_per_section:file_section*files_per_section]

# read in the parquet file
df = pd.concat(pd.read_parquet(f) for f in file_sublist)
df = df.dropna(subset = ["scraped_at"])
df["scraped_at_year"] = pd.to_datetime(df['scraped_at'], unit='s').dt.year

# partition the data
partitions = df.groupby(["state", "scraped_at_year"])

# write each partition to its own directory
for (state, scraped_at_year), partition in partitions:
    partition_path = os.path.join(output_dir, state, str(scraped_at_year))
    os.makedirs(partition_path, exist_ok=True)
    partition.to_parquet(f"{partition_path}/file_{file_section}.parquet")
