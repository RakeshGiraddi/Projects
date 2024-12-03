# Importing the modules

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import TimestampType, IntegerType, FloatType, ArrayType,LongType
from pyspark.sql import functions as func

# Initializing Spark Session

spark = SparkSession  \
    .builder  \
    .appName("StructuredSocketRead")  \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Defining the schema

schema1 = StructType([
        StructField("booking_id", StringType()),
        StructField("customer_id",  LongType()),
        StructField("driver_id",  LongType()),
        StructField("customer_app_version", StringType()),
        StructField("customer_phone_os_version", StringType()),
        StructField("pickup_lat", DoubleType()),
        StructField("pickup_lon", DoubleType()),
        StructField("drop_lat", DoubleType()),
        StructField("drop_lon", DoubleType()),
        StructField("pickup_timestamp", TimestampType()),
        StructField("drop_timestamp", TimestampType()),
        StructField("trip_fare", IntegerType()),
        StructField("tip_amount", IntegerType()),
        StructField("currency_code", StringType()),
        StructField("cab_color", StringType()),
        StructField("cab_registration_no", StringType()),
        StructField("customer_rating_by_driver", IntegerType()),
        StructField("rating_by_customer", IntegerType()),
        StructField("passenger_count", IntegerType())
        ])

# Reading the file "part-m-00000" in bookings_data folder in hadoop

df=spark.read.csv("bookings_data/part-m-00000", schema=schema1)

# Creating data from column "pickup_date" and "pickup_timestamp"

df = df.withColumn("pickup_date", func.to_date(func.col("pickup_timestamp")))

# Group the data by "pickup_date"

date = df.groupBy('pickup_date').count()

# Saving the datewise total booking data in .csv format

date.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save("datewise_aggregated_data/")

# Saving the booking data in .csv format

df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('booking_data_csv/', header = 'true')