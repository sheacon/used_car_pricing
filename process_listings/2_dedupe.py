
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
df = df.loc[mask]

# try unnesting
df["hvf_options"] = df["hvf_options"].apply(str)
df["listed_options"] = df["listed_options"].apply(str)


# unnest hvf_options
#df["hvf_standard"] = df["hvf_options"].apply(lambda x: x[0])
#df["hvf_optional"] = df["hvf_options"].apply(lambda x: x[1])
#df = df.drop(["hvf_options"], axis=1)

# write out
output_dir = "/data/p_dsi/capstone_projects/shea/2_deduped"
df.to_parquet(f"{output_dir}/{state}_{year}.parquet")


