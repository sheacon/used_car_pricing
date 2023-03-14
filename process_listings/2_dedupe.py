
#export ARROW_MEMORY_POOL_MAX_MEMORY=4GB

import sys
import pyarrow as pa
import pyarrow.parquet as pq

state = sys.argv[1]
year = int(sys.argv[2])

# debug
#state = "TN"
#year = 2

# determine year
years = ['2018', '2019', '2020', '2021', '2022']
year = years[year]

# file structure
base_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned"
input_dir = f"{base_dir}/state={state}/status_date_year={year}/"
#input_dir = "/data/p_dsi/capstone_projects/shea/1_partitioned/state=TN/status_date_year=2020"

# read files
table = pq.read_table(input_dir)
schema = table.schema

# dedupe vin by status_date
sorted_table = table.sort_by([('vin', 'ascending'), ('status_date', 'descending')])
group_cols = [f.name for f in schema if f.name not in ('vin', 'status_date')]
grouped_table = sorted_table.groupby('vin').aggregate([
    ('status_date', 'first'),
    *[(c, 'first') for c in group_cols],
])

# write out
output_dir = "/data/p_dsi/capstone_projects/shea/2_deduped/"
output_file = f"{state}_{year}.parquet"
pq.write_table(table, output_dir+output_file)
)