
import sys
import pandas as pd
import pyarrow

state = sys.argv[1]
year = int(sys.argv[2])

# debug
#state = "TN"
#year = 6
#file = "fff11ec781f44c9ea9cbf02476f44d39-0.parquet"
#df = pd.read_parquet(input_dir+file)

# determine year
years = ['2017','2018', '2019', '2020', '2021', '2022','2023']
year = years[year]

# file structure
base_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned"
input_dir = f"{base_dir}/state={state}/status_date_year={year}/"

# read files
df = pd.read_parquet(input_dir)

mask = df['status_date'] == df.groupby('vin')['status_date'].transform(max)
deduped_df = df.loc[mask]

# write out
output_dir = "/data/p_dsi/capstone_projects/shea/2_deduped"
deduped_df.to_parquet(f"{output_dir}/{state}_{year}.parquet")


