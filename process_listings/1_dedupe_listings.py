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
input_dir = '/data/p_dsi/capstone_projects/shea/processed/large_sample'
output_dir = '/data/p_dsi/capstone_projects/shea/deduped/' + job_id

# read parquet
df = spark.read.parquet(input_dir)

# size
print((df.count(), len(df.columns)))

# create a window partitioned by "vin" and sorted by "status_date" in descending order
window = Window.partitionBy("vin").orderBy(F.col("status_date").desc())

# dedupe
df2 = df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col("row_number") == 1) \
        .drop("row_number")

# write csv
df2.write.parquet(output_dir, mode = 'overwrite')

sc.stop()

