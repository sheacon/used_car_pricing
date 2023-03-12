
# salloc --time=02:00:00 --ntasks=1 --cpus-per-task=12 --mem=480G

conf = spark.sparkContext.getConf()
conf.set("spark.debug.maxToStringFields", "100")

from pathlib import Path

# Directory path containing parquet files
parquet_dir = '/data/p_dsi/capstone_projects/shea/0_processed_listings/full_data'

# Use Path object to select only parquet files in the directory
path = Path(parquet_dir)
parquet_files = [str(file) for file in path.glob('*.parquet')]

# Load all the parquet files in the directory into a DataFrame
df = spark.read.parquet(*parquet_files)
# drop na vins
df = df.dropna(subset=["vin"])

# df size
print("Number of rows: ", df.count())
print("Number of columns: ", len(df.columns))


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define a window specification
window_spec = Window.partitionBy("vin").orderBy(df["status_date"].desc())

# Add a row number column to the DataFrame
df = df.withColumn("row_number", row_number().over(window_spec))

# Keep only the rows with the largest status_date for each vin
df = df.filter(df.row_number == 1).drop("row_number")

# df
print("Number of rows: ", df.count())
print("Number of columns: ", len(df.columns))

output_dir = '/data/p_dsi/capstone_projects/shea/1_deduped_listings/full_data'

df.write.parquet(output_dir, mode = 'overwrite')
