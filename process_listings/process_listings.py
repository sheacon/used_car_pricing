import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

sc = SparkContext()

# construct spark session instance
spark = SparkSession(sc).builder \
    .master("spark://master:7077") \
    .appName("Process Listings") \
    .config("spark.debug.maxToStringFields", "150") \
    .getOrCreate()

# input and output
input_file = '/data/p_dsi/capstone_projects/shea/mc_listings_large_sample.csv'
output_dir = '/data/p_dsi/capstone_projects/shea/processed_large_sample/'

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
df = spark.read.csv(input_file, schema = schema, header = True, sep = ',', quote = '"', escape='"')

# size
print((df.count(), len(df.columns)))

# create a window partitioned by "vin" and sorted by "status_date" in descending order
window = Window.partitionBy("vin").orderBy(F.col("status_date").desc())

# dedupe
df = df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col("row_number") == 1) \
        .drop("row_number")

from pyspark.sql.functions import col, size, split, isnull, udf, length, regexp_replace
import json

# parse high value features from nested json to compact dictionary
def hvf_parse(hvf_field):
    if hvf_field:
        hvf_items = json.loads(hvf_field)
        options = {'Standard': {}, 'Optional': {}}
        for item in hvf_items:
            if item['category'] not in options[item['type']]:
                options[item['type']][item['category']] = []
            options[item['type']][item['category']].append(item['description'])
        hvf_field = str(options)
    return hvf_field

# convert to udf
hvf_parse_udf = udf(hvf_parse, StringType())

# compile all options
def total_options(options, features):
    combined = '|'.join(list(set(str(options).split('|') + str(features).split('|'))))
    return combined

# convert to udf
total_options_udf = udf(total_options, StringType())

# unneeded cols to drop
drop_cols = ['more_info'
            ,'model_code'
            ,'dealer_id'
            ,'street'
            ,'country'
            ,'seller_phone'
            ,'seller_email'
            ,'seller_type'
            ,'listing_type'
            ,'inventory_type'
            ,'car_address'
            ,'car_street'
            ,'options'
            ,'features'
            ,'photo_links'
            ,'photo_url'
            ,'in_transit'
            ,'in_transit_at'
            ,'in_transit_days'
            ,'high_value_features'
            ]

df = (df
        # photo links handling
        .withColumn('photo_links_count', size(split(col('photo_links'), r'\|'))) # count photo links
        .replace(0,1,'photo_links_count') # fix count for single photos
        .replace(-1,0,'photo_links_count') # fix count for NAs

        # main photo handling
        .withColumn('photo_main', ~isnull(col('photo_url')))

        # combined options
        .withColumn('total_options',total_options_udf(col('options'),col('features')))

        # hvf
        .withColumn('hvf_parsed',hvf_parse_udf(col('high_value_features')))

        # unneded columns
        .drop(*drop_cols)
        )

# write csv
df.write.csv(output_dir,header = True, sep = ',', quote = '"', escape='"', mode = 'overwrite')

sc.stop()

