import sys
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

INGEST_DATE = sys.argv[1]
ingest_date_list = INGEST_DATE.split('-')
YEAR = int(ingest_date_list[0])
MONTH = int(ingest_date_list[1])
DAY = int(ingest_date_list[2])

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("HDFS file processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

raw_data_schema = StructType(fields=[
    StructField("OrderID", IntegerType(), nullable=False),
    StructField("CustomerID_sk", IntegerType()),
    StructField("EmployeeID", IntegerType()),
    StructField("OrderDate", TimestampType()),
    StructField("RequiredDate", TimestampType()),
    StructField("ShippedDate", TimestampType()),
    StructField("ShipVia", IntegerType()),
    StructField("Freight", FloatType()),
    StructField("ProductID", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Quantity", IntegerType()),
    StructField("Discount", FloatType()),
    StructField("CustomerID_bk", IntegerType()),
    StructField("FirstName_cust", StringType()),
    StructField("LastName_cust", StringType()),
    StructField("Address_cust", StringType()),
    StructField("City_cust", StringType()),
    StructField("PostalCode_cust", StringType()),
    StructField("Country_cust", StringType()),
    StructField("Phone_cust", StringType()),
])

df = spark.read.load(f"hdfs://namenode:9000/NewStoreRawData/Year={YEAR}/Month={MONTH}/Day={DAY}/", format='parquet')

df = df.withColumn('Year', lit(YEAR)) \
    .withColumn('Month', lit(MONTH)) \
    .withColumn('Day', lit(DAY))


# Write to Hive Table
df.write.mode("overwrite").format('parquet').partitionBy("Year", "Month", "Day").saveAsTable("test_db.test_table2")