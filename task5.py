import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import sum
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
from graphframes import *

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark.sql.functions import month
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import avg
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import count
from pyspark.sql.functions import concat_ws, lit, col

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    rideshare_data = spark.read.option("header", "true").csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup_df = spark.read.option("header", "true").csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    
#Task 1
    
#Loading the Data
rideshare_data = spark.read.option("header", "true").csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
taxi_zone_lookup_df = spark.read.option("header", "true").csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
        
# Select and alias columns for pickup
renamed_pickup_fields = taxi_zone_lookup_df.select(
    col("LocationID").alias("pickup_LocationID"),
    col("Borough").alias("Pickup_Borough"),
    col("Zone").alias("Pickup_Zone"),
    col("service_zone").alias("Pickup_service_zone")
)

# Select and alias columns for dropoff
renamed_dropoff_fields = taxi_zone_lookup_df.select(
    col("LocationID").alias("dropoff_LocationID"),
    col("Borough").alias("Dropoff_Borough"),
    col("Zone").alias("Dropoff_Zone"),
    col("service_zone").alias("Dropoff_service_zone")
)

# First join on pickup location
first_joined_df = rideshare_data.join(renamed_pickup_fields, rideshare_data.pickup_location == renamed_pickup_fields.pickup_LocationID)

# Second join on dropoff location
main_df = first_joined_df.join(renamed_dropoff_fields, first_joined_df.dropoff_location == renamed_dropoff_fields.dropoff_LocationID)

#Convert to UNIX timestamp
main_df = main_df.withColumn("date", from_unixtime("date", "yyyy-MM-dd"))

# Print the number of rows in main_df
print("Number of rows:", main_df.count())

# Drop the locationID fields which are not part of the schema output
main_df = main_df.drop('dropoff_LocationID', 'pickup_LocationID')
    
# Print the schema of main_df
main_df.printSchema()

# Print the updated UNIX date column with some other fields
main_df.select("trip_length","total_ride_time","time_of_day", "date").show()


#Task 5

# January data filtering
jan_data = main_df.filter(month("date") == 1)

# Using January data, calculate average waiting time 
avg_wait_time_jan = jan_data.withColumn("day", dayofmonth("date")) \
                            .groupBy("day") \
                            .agg(avg("request_to_pickup").alias("average_waiting_time")) \
                            .orderBy("day")
avg_wait_time_jan.show()
df_avg_wt_jan = avg_wait_time_jan.coalesce(1)
df_avg_wt_jan.write.mode("overwrite").option("header", "true").csv(f"s3a://{s3_bucket}/folder_avg_wait_time_jan_csv")

#Calculating days avg wait time exceeds 300s
days_over_300s = avg_wait_time_jan.filter(col("average_waiting_time") > 300).select("day", "average_waiting_time")
days_over_300s.show()