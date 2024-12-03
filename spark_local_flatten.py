# Importing the modules

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initializing Spark Session

spark = SparkSession.builder \
    .master("local") \
    .appName("CapstoneProject") \
    .getOrCreate()

# Reading json data into dataframe 

clickstream_df = spark.read.json('clickstream_data/part-00000-d8df90ac-1ec9-4014-b633-a20d66f44a32-c000.json')




# Extrating columns from json value in dataframe and create new dataframe with new cloumns

clickstream_df = clickstream_df.select(\
	get_json_object(clickstream_df["value_str"],"$.customer_id").alias("customer_id"),\
	get_json_object(clickstream_df["value_str"],"$.app_version").alias("app_version"),\
	get_json_object(clickstream_df["value_str"],"$.OS_version").alias("OS_version"),\
	get_json_object(clickstream_df["value_str"],"$.lat").alias("lat"),\
	get_json_object(clickstream_df["value_str"],"$.lon").alias("lon"),\
	get_json_object(clickstream_df["value_str"],"$.page_id").alias("page_id"),\
	get_json_object(clickstream_df["value_str"],"$.button_id").alias("button_id"),\
	get_json_object(clickstream_df["value_str"],"$.is_button_click").alias("is_button_click"),\
	get_json_object(clickstream_df["value_str"],"$.is_page_view").alias("is_page_view"),\
	get_json_object(clickstream_df["value_str"],"$.is_scroll_up").alias("is_scroll_up"),\
	get_json_object(clickstream_df["value_str"],"$.is_scroll_down").alias("is_scroll_down"),\
	get_json_object(clickstream_df["value_str"],"$.timestamp\n").alias("timestamp"),\
	)

# Printing the Dataframe schema 

print(clickstream_df.schema)	

# Printing 10 records from the dataframe

clickstream_df.show(10)

# Saving the dataframe to csv file with headers in local file directory

clickstream_df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('clickstream_data_flatten/', header = 'true')
