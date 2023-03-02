import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import from_unixtime
import sys


#job_id = sys.argv[1]

#sc = SparkContext()

# construct spark session instance
spark = SparkSession(sc).builder \
    .appName("Dedupe Listings") \
    .master("local") \
    .config("spark.driver.memory", "7g") \
    .config("spark.executor.memory", "7g") \
    .config("spark.debug.maxToStringFields", "150") \
    .getOrCreate()

# input and output
listings_file = '/data/p_dsi/capstone_projects/shea/0_processed_listings/small_sample'
registrations_file = '/data/p_dsi/capstone_projects/shea/mvr_vin_dates.parquet'
output_dir = '/data/p_dsi/capstone_projects/shea/1_deduped_listings/'

# read parquet
listings = spark.read.parquet(listings_file)
registrations = spark.read.parquet(registrations_file)

# remove records with null vin
listings = listings.filter(listings['vin'].isNotNull())

# dupe registrations issue
registrations.count()
registrations.dropDuplicates().count()
registrations.count()

# record count
listings.count()
registrations.count()

# unique vin count
listings.select(F.countDistinct("vin")).collect()[0][0]

# duplicate vins
duplicates = listings.groupBy("vin").agg(F.count("*").alias("count")).filter("count > 1")
duplicates.show()

# duplicate records
duplicate_listings = duplicates.join(listings, "vin", "inner").select(['vin','status_date','scraped_at','first_scraped_at']).sort('vin','status_date')
duplicate_listings = duplicate_listings \
            .withColumn('first_scraped_at', from_unixtime('first_scraped_at')) \
            .withColumn('scraped_at', from_unixtime('scraped_at')) \
            .withColumn('status_date', from_unixtime('status_date'))
duplicate_listings.show()









from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, max, when
from pyspark.sql.window import Window

# Define windows for each table
listings_window = Window.partitionBy('vin').orderBy(col('status_date').desc())
registrations_window = Window.partitionBy('vin').orderBy(col('mvr_sale_date').asc())

latest_listings = (
    duplicate_listings
    .withColumn('rn', row_number().over(listings_window))
    .join(
        registrations,
        on='vin',
        how='inner'
    )
    .filter(col('status_date') <= col('mvr_sale_date'))
    .select('vin', 'status_date', 'scraped_at', 'first_scraped_at', 'mvr_sale_date')
).show()











# create temp views
duplicate_listings.createOrReplaceTempView("duplicate_listings")
registrations.createOrReplaceTempView("registrations")


query = """
SELECT
    registrations.*,
    duplicate_listings.*,
    ROW_NUMBER() OVER(
        PARTITION BY registrations.vin
        ORDER BY
            duplicate_listings.status_date DESC
    )
FROM
    registrations
    LEFT JOIN duplicate_listings ON registrations.vin = duplicate_listings.vin
    AND registrations.mvr_sale_date >= duplicate_listings.status_date
"""

spark.sql(query).show()


query = """
SELECT
    *
FROM
    (
        SELECT
            registrations.*,
            duplicate_listings.*,
            ROW_NUMBER() OVER(
                PARTITION BY registrations.id
                ORDER BY
                    duplicate_listings.timestamp DESC
            )
        FROM
            registrations
            LEFT JOIN duplicate_listings ON registrations.vehicle_id = duplicate_listings.vehicle_id
            AND registrations.timestamp >= duplicate_listings.timestamp
    ) a
WHERE
    a.ROW_NUMBER = 1;
"""





# write csv
df.write.parquet(output_dir, mode = 'overwrite')

sc.stop()

