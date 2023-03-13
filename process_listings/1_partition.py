import pandas as pd
import pyarrow
import os
import sys
from glob import glob

input_dir = sys.argv[1]
file_section = int(sys.argv[2])
file_sections = int(sys.argv[3])
output_dir = sys.argv[4]

# debug
#input_dir = "/data/p_dsi/capstone_projects/shea/0_processed/full_data/"
#output_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned/"
#file_section = 20

file_list = glob(input_dir + '*.parquet')
files_per_section = len(file_list)//file_sections
file_sublist = file_list[(file_section-1)*files_per_section:file_section*files_per_section]

# debug
#len(file_list)
#(file_section-1)*files_per_section
#file_section*files_per_section
#file_sublist = file_sublist[0:10]

# read in the parquet file
df = pd.concat(pd.read_parquet(f) for f in file_sublist).reset_index()
df = df.dropna(subset = ["scraped_at"])
df["scraped_at_year"] = pd.to_datetime(df['scraped_at'], unit='s').dt.year

# unnest hvf features
df["hvf_standard"] = df["hvf_options"].apply(lambda x: x[0])
df["hvf_optional"] = df["hvf_options"].apply(lambda x: x[1])
df = df.drop(["hvf_options"], axis=1)

# drop other unneeded columns
df = df.drop(['loan_term', 'loan_apr', 'l_down_pay', 'l_emi',
       'f_down_pay', 'f_down_pay_per', 'f_emi', 'lease_term',
       'index','id', 'heading', 'msrp', 'stock_no', 'engine',
       'engine_measure', 'engine_aspiration', 'speeds', 'interior_color',
       'exterior_color', 'taxonomy_vin', 'source', 'seller_name',
       'car_seller_name', 'car_city', 'car_state', 'car_zip',
        'car_latitude', 'car_longitude', 'dom', 'dom_180',
        'dom_active','carfax_1_owner','carfax_clean_title'], axis=1)

# unneeded
#df = df[df["currency_indicator"] == "USD"]
#df = df[df["miles_indicator"] == "MILES"]
#df = df.drop(["currency_indicator","miles_indicator"], axis=1)

# partition the data
partitions = df.groupby(["state", "scraped_at_year"])

# write each partition to its own directory
for (state, scraped_at_year), partition in partitions:
#    if scraped_at_year < 2018:
#        continue
    partition_path = os.path.join(output_dir, state, str(scraped_at_year))
    os.makedirs(partition_path, exist_ok=True)
    partition.to_parquet(f"{partition_path}/file_{file_section}.parquet")
