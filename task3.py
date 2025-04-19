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

#Task 2
    
# Count the number of trips per business per month by extracting month from 'date'. Then collect the data for the visualisation.
no_trips_per_bus_month = main_df.groupBy("business", month("date").alias("month")).count()
trips_data = no_trips_per_bus_month.collect()

# Sum platform profits for each business in each month
platform_profits_per_bus_month = main_df.groupBy("business", month("date").alias("month")).agg(sum("rideshare_profit").alias("total_profit"))

# Collect the data to use in histogram
profits_data = platform_profits_per_bus_month.collect()
main_df = main_df.withColumn("driver_total_pay", col("driver_total_pay").cast(DoubleType()))
main_df = main_df.withColumn("month", month("date"))
driver_earnings_per_bus_month = main_df.groupBy("business", "month").sum("driver_total_pay").orderBy("business", "month")

# Group by Pickup_Borough and month, count the trips
no_of_pickUps = main_df.groupBy("Pickup_Borough", "month").count().withColumnRenamed("count", "trip_count")
    
#Task 3
    
# top 5 popular pickup boroughs each month
pickUp_partition = Window.partitionBy("month").orderBy(col("trip_count").desc())# Window specification to partition by month and order by trip_count
# Apply the window specification and filter for top 5 for each month
popular_pickUp_boroughs = no_of_pickUps.withColumn("rank", rank().over(pickUp_partition)) \
.filter(col("rank") <= 5) \
.select("Pickup_Borough", "month", "trip_count") \
.orderBy("month", "trip_count", ascending=[True, False])
popular_pickUp_boroughs.show(25)#top 5 for each month in the dataset, since there are 5 months in the dataset, 5x5 = 25

# top 5 popular drop off boroughs each month
dropOff_partition = Window.partitionBy("month").orderBy(col("trip_count").desc())# Window specification to partition by month and order by trip_count for dropoff
dropoff_counts = main_df.groupBy("Dropoff_Borough", "month").count().withColumnRenamed("count", "trip_count")# Group by Dropoff_Borough and month, count the trips
# Apply the window specification and filter for top 5 for each month
top_dropoff_boroughs = dropoff_counts.withColumn("rank", rank().over(dropOff_partition)) \
.filter(col("rank") <= 5) \
.select("Dropoff_Borough", "month", "trip_count") \
.orderBy("month", "trip_count", ascending=[True, False])
top_dropoff_boroughs.show(25)#top 5 for each month in the dataset, since there are 5 months in the dataset, 5x5 = 25


# Concatenate Pickup_Borough and Dropoff_Borough to create a route column
main_df = main_df.withColumn("Route", concat_ws(" to ", "Pickup_Borough", "Dropoff_Borough"))

# Group by Route and sum the driver_total_pay
route_profits = main_df.groupBy("Route").sum("driver_total_pay").withColumnRenamed("sum(driver_total_pay)", "total_profit")

# Order by total_profit and take the top 30
top_routes = route_profits.orderBy("total_profit", ascending=False)

top_routes.show(30,truncate=False)


