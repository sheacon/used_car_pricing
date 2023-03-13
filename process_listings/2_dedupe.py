
import sys
import pandas as pd
import pyarrow
from glob import glob

state = sys.argv[1]
year = int(sys.argv[2])

# debug
#state = "TN"
#year = 0

# determine year
years = ['2018', '2019', '2020', '2021', '2022']
year = years[year]

# file structure
base_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned"
input_dir = f"{base_dir}/{state}/{year}/"
file_list = glob(input_dir + '*.parquet')

# read files
df = pd.concat(pd.read_parquet(f) for f in file_list)

# dedupe vin by status_date
mask = df['status_date'] == df.groupby('vin')['status_date'].transform(max)
deduped_df = df.loc[mask]

# write out
output_dir = "/data/p_dsi/capstone_projects/shea/2_deduped"
deduped_df.to_parquet(f"{output_dir}/{state}_{year}.parquet")

# debug read
# pd.read_parquet("/data/p_dsi/capstone_projects/shea/1_partitioned/TN/2018/file_4.parquet")
#df = pd.read_parquet("/data/p_dsi/capstone_projects/shea/2_deduped/TN_2018.parquet")