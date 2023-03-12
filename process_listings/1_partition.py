import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()

# construct spark session instance
spark = SparkSession(sc).builder \
    .master("spark://master:7077") \
    .appName("Partition Listings") \
    .config("spark.debug.maxToStringFields", "150") \
    .getOrCreate()

# input and output
input_dir = '/data/p_dsi/capstone_projects/shea/0_processed/full_data'
output_dir = '/data/p_dsi/capstone_projects/shea/1_partitioned'

# read in parquet files and partition by "state"
df = spark.read.parquet(input_dir)

# write each partition to its own directory
df.write.partitionBy("state").parquet(output_dir, mode="overwrite")

# end
sc.stop()

