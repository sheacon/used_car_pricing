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
#output_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned/"

file_list = glob(input_dir + '*.parquet')
files_per_section = len(file_list)//99
file_sublist = file_list[(file_section-1)*files_per_section:file_section*files_per_section]

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
df = df[df["currency_indicator"] == "USD"]
df = df[df["miles_indicator"] == "MILES"]
df = df.drop(["currency_indicator","miles_indicator"], axis=1)

# partition the data
partitions = df.groupby(["state", "scraped_at_year"])

schema = StructType([
    StructField('vin',StringType(),True)
    ,StructField('price',IntegerType(),True)
    ,StructField('miles',IntegerType(),True)
    ,StructField('year',ShortType(),True)
    ,StructField('make',StringType(),True)
    ,StructField('model',StringType(),True)
    ,StructField('trim',StringType(),True)
    ,StructField('vehicle_type',StringType(),True)
    ,StructField('body_type',StringType(),True)
    ,StructField('body_subtype',StringType(),True)
    ,StructField('drivetrain',StringType(),True)
    ,StructField('fuel_type',StringType(),True)
    ,StructField('engine_block',StringType(),True)
    ,StructField('engine_size',StringType(),True)
    ,StructField('transmission',StringType(),True)
    ,StructField('doors',ByteType(),True)
    ,StructField('cylinders',ByteType(),True)
    ,StructField('city_mpg',ByteType(),True)
    ,StructField('highway_mpg',ByteType(),True)
    ,StructField('base_exterior_color',StringType(),True)
    ,StructField('base_interior_color',StringType(),True)
    ,StructField('is_certified',ByteType(),True)
    ,StructField('is_transfer',ByteType(),True)
    ,StructField('scraped_at',StringType(),True)
    ,StructField('status_date',StringType(),True)
    ,StructField('first_scraped_at',StringType(),True)
    ,StructField('city',StringType(),True)
    ,StructField('state',StringType(),True)
    ,StructField('zip',StringType(),True)
    ,StructField('latitude',FloatType(),True)
    ,StructField('longitude',FloatType(),True)
    ,StructField('dealer_type',StringType(),True)
    ,StructField('seller_comments',StringType(),True)
    ,StructField('photo_links_count',IntegerType(),True)
    ,StructField('photo_main',IntegerType(),True)
    ,StructField('listed_options',ArrayType(StringType(),True),True)
    ,StructField('scraped_at_year',IntegerType(),True)
    ,StructField('hvf_standard',ArrayType(StringType(),True),True)
    ,StructField('hvf_optional',ArrayType(StringType(),True),True)
    ])

# write each partition to its own directory
for (state, scraped_at_year), partition in partitions:
    partition_path = os.path.join(output_dir, state, str(scraped_at_year))
    os.makedirs(partition_path, exist_ok=True)
    partition.to_parquet(f"{partition_path}/file_{file_section}.parquet", mode="overwrite", schema=df.schema)
