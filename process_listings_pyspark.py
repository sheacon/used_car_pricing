
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

sc = SparkContext()

# construct spark session instance
spark = SparkSession(sc).builder \
    .master("local") \
    .appName("Process Listings") \
    .config("spark.debug.maxToStringFields", "100") \
    .getOrCreate()

# file path
#file = '/data/p_dsi/capstone_projects/shea/mc_listings_extract.csv.gz'
file = 'hdfs:///user/conawws1/mc_listings_extract.csv.gz'

# define schema
schema = StructType([
    StructField('id',StringType(),True)
    ,StructField('vin',StringType(),True)
    ,StructField('heading',StringType(),True)
    ,StructField('more_info',StringType(),True)
    ,StructField('price',IntegerType(),True)
    ,StructField('msrp',IntegerType(),True)
    ,StructField('miles',IntegerType(),True)
    ,StructField('stock_no',StringType(),True)
    ,StructField('year',ShortType(),True)
    ,StructField('make',StringType(),True)
    ,StructField('model',StringType(),True)
    ,StructField('trim',StringType(),True)
    ,StructField('vehicle_type',StringType(),True)
    ,StructField('body_type',StringType(),True)
    ,StructField('body_subtype',StringType(),True)
    ,StructField('drivetrain',StringType(),True)
    ,StructField('fuel_type',StringType(),True)
    ,StructField('engine',StringType(),True)
    ,StructField('engine_block',StringType(),True)
    ,StructField('engine_size',StringType(),True)
    ,StructField('engine_measure',StringType(),True)
    ,StructField('engine_aspiration',StringType(),True)
    ,StructField('transmission',StringType(),True)
    ,StructField('speeds',ByteType(),True)
    ,StructField('doors',ByteType(),True)
    ,StructField('cylinders',ByteType(),True)
    ,StructField('city_mpg',ByteType(),True)
    ,StructField('highway_mpg',ByteType(),True)
    ,StructField('interior_color',StringType(),True)
    ,StructField('exterior_color',StringType(),True)
    ,StructField('base_exterior_color',StringType(),True)
    ,StructField('base_interior_color',StringType(),True)
    ,StructField('is_certified',ByteType(),True) # try BooleanType
    ,StructField('is_transfer',ByteType(),True) # try BooleanType
    ,StructField('taxonomy_vin',StringType(),True)
    ,StructField('model_code',StringType(),True)
    ,StructField('scraped_at',StringType(),True)
    ,StructField('status_date',StringType(),True)
    ,StructField('first_scraped_at',StringType(),True)
    ,StructField('dealer_id',StringType(),True)
    ,StructField('source',StringType(),True)
    ,StructField('seller_name',StringType(),True)
    ,StructField('street',StringType(),True)
    ,StructField('city',StringType(),True)
    ,StructField('state',StringType(),True)
    ,StructField('zip',StringType(),True)
    ,StructField('latitude',FloatType(),True)
    ,StructField('longitude',FloatType(),True)
    ,StructField('country',StringType(),True)
    ,StructField('seller_phone',StringType(),True)
    ,StructField('seller_email',StringType(),True)
    ,StructField('seller_type',StringType(),True)
    ,StructField('listing_type',StringType(),True)
    ,StructField('inventory_type',StringType(),True)
    ,StructField('dealer_type',StringType(),True)
    ,StructField('car_seller_name',StringType(),True)
    ,StructField('car_address',StringType(),True)
    ,StructField('car_street',StringType(),True)
    ,StructField('car_city',StringType(),True)
    ,StructField('car_state',StringType(),True)
    ,StructField('car_zip',StringType(),True)
    ,StructField('car_latitude',FloatType(),True)
    ,StructField('car_longitude',FloatType(),True)
    ,StructField('seller_comments',StringType(),True)
    ,StructField('options',StringType(),True)
    ,StructField('features',StringType(),True)
    ,StructField('photo_links',StringType(),True)
    ,StructField('photo_url',StringType(),True)
    ,StructField('dom',ShortType(),True)
    ,StructField('dom_180',ShortType(),True)
    ,StructField('dom_active',ShortType(),True)
    ,StructField('currency_indicator',StringType(),True)
    ,StructField('miles_indicator',StringType(),True)
    ,StructField('carfax_1_owner',ByteType(),True) # try BooleanType
    ,StructField('carfax_clean_title',ByteType(),True) # try BooleanType
    ,StructField('loan_term',ShortType(),True)
    ,StructField('loan_apr',FloatType(),True)
    ,StructField('l_down_pay',FloatType(),True)
    ,StructField('l_emi',FloatType(),True)
    ,StructField('f_down_pay',FloatType(),True)
    ,StructField('f_down_pay_per',FloatType(),True)
    ,StructField('f_emi',FloatType(),True)
    ,StructField('lease_term',ShortType(),True)
    ,StructField('in_transit',ByteType(),True) # try BooleanType
    ,StructField('in_transit_at',StringType(),True)
    ,StructField('in_transit_days',IntegerType(),True)
    ,StructField('high_value_features',StringType(),True)
    ])

# read csv
df = spark.read.csv(file, schema = schema, sep = ',', quote = '"')
## PARSING PROBLEM, need to use quotations as field delimiter
    # example vins (1C6RR6YT5JS119710, 4T1B11HK2KU777711)

# size
print((df.count(), len(df.columns)))

# preview
df.take(5)

# confirm 
df.printSchema()

from pyspark.sql.functions import col, size, split, isnull

df2 = (df
        .drop('more_info') # drop listing url
        .withColumn('photo_links_count', size(split(col('photo_links'), r'\|'))) # count photo links
        .drop('photo_links') # drop photo links
        .replace(-1,0,'photo_links_count') # fix count for NAs
        .withColumn('photo_main', ~isnull(col('photo_url')))
        )
#       .sample(fraction = 0.01, withReplacement = False)


df2.select('vin','photo_url','photo_main').take(10)


df2.where("vin == '1C6RR6YT5JS119710'").select(['vin','dom']).show()

sc.stop()

