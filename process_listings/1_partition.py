
import sys
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

subfolder = sys.argv[1]

# debug
#subfolder = "20"

base_input_dir = "/data/p_dsi/capstone_projects/shea/0_processed/full_data/"
input_dir = base_input_dir + subfolder

output_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned/"

read_cols = ['vin', 'price', 'miles', 'year', 'make', 'model', 'trim',
       'vehicle_type', 'body_type', 'body_subtype', 'drivetrain', 'fuel_type',
       'engine_block', 'engine_size', 'transmission', 'doors', 'cylinders',
       'city_mpg', 'highway_mpg', 'base_exterior_color', 'base_interior_color',
       'is_certified', 'is_transfer', 'scraped_at', 'status_date',
       'first_scraped_at', 'city', 'state', 'zip', 'latitude', 'longitude',
       'dealer_type', 'seller_comments', 'currency_indicator',
       'miles_indicator', 'photo_links_count', 'listed_options',
       'hvf_options']

# read in the parquet files
table = pq.read_table(input_dir, columns=read_cols)

# year
status_date = pa.compute.fill_null(table["status_date"], "0").to_numpy()
years = [datetime.fromtimestamp(int(ts)).year for ts in status_date]
table = table.append_column("status_date_year", pa.array(years))

# write each partition to its own directory
partition_cols = ['state', 'status_date_year']
pq.write_to_dataset(
    table=table,
    root_path=output_dir,
    partition_cols=partition_cols,
    compression='snappy'
)
