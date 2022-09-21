from pyspark.sql import SparkSession
from os.path import expanduser, join, abspath
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType


# spark-submit project_scripts/test/02_hdfs_to_hive_by_spark.py

INGEST_DATE = '2022-07-04'
ingest_date_list = INGEST_DATE.split('-')
YEAR = int(ingest_date_list[0])
MONTH = int(ingest_date_list[1])
DAY = int(ingest_date_list[2])

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("HDFS File processing") \
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


df = spark.read.load(f"project_scripts/test/output/NewStoreRawData/Year={YEAR}/Month={MONTH}/Day={DAY}/", header=True,
                     format='csv', schema=raw_data_schema)

df.show(truncate=False)