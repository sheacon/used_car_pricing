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

listings.createOrReplaceTempView("listings")
registrations.createOrReplaceTempView("registrations")

query = """
SELECT registrations.*, listings.*
FROM registrations
LEFT JOIN (
    SELECT DISTINCT ON (vin) *
    FROM listings
    WHERE status_date <= registrations.mvr_sale_date
    ORDER BY vin, status_date DESC
) AS listings
ON registrations.vin = listings.vin
"""

df = spark.sql(query)

# write csv
df.write.parquet(output_dir, mode = 'overwrite')

sc.stop()

