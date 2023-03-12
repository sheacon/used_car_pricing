
# salloc --time=02:00:00 --mem=8G --cpus-per-task=2 --constraint=skylake
# module load GCCcore/.8.2.0 Spark/2.4.0

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession(sc).builder \
    .appName("Count Listings") \
    .master("local[6]") \
    .config("spark.debug.maxToStringFields", "150") \
    .getOrCreate()


# input and output
listings_file = '/data/p_dsi/capstone_projects/shea/0_processed_listings/full_data'

# read parquet
listings = spark.read.parquet(listings_file)

# record count
listings.count()


