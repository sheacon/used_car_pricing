import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys

job_id = sys.argv[1]

sc = SparkContext()

# construct spark session instance
spark = SparkSession(sc).builder \
    .master("spark://master:7077") \
    .appName("Process Listings") \
    .config("spark.debug.maxToStringFields", "150") \
    .getOrCreate()

# input and output
listings = '/data/p_dsi/capstone_projects/shea/processed/large_sample'
registrations = '/data/p_dsi/capstone_projects/shea/processed/mvr_vin_dates.parquet'
output_dir = '/data/p_dsi/capstone_projects/shea/processed/deduped/' + job_id

# read parquet
listings = spark.read.parquet(listings)
registrations = spark.read.parquet(registrations)


from pyspark.sql.functions import max, col

# Create a window function to partition the listings by vehicle_id and order by timestamp in descending order
window = Window.partitionBy("vin").orderBy(col("status_date").desc())

# Use the window function to get the most recent listing for each vehicle that occurred before or at the time of the registration
latest_listing = listings.where(col("status_date") <= col("mvr_sale_date")).select("*", row_number().over(window).alias("rn")).filter(col("rn") == 1)

# Join the registrations table with the latest_listing table on vehicle_id
result = registrations.join(latest_listing, "vin", "left")

# write csv
df.write.parquet(output_dir, mode = 'overwrite')

sc.stop()

