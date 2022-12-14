import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

spark = SparkSession.builder.getOrCreate()

# spark-submit --jars project_scripts/lib/enu/mssql-jdbc-11.2.0.jre8.jar project_scripts/01_mssql_test.py

mssql_url = "jdbc:sqlserver://192.168.0.103:1433;databaseName=NewStoreDB;encrypt=true;trustServerCertificate=true;"
username = "Daniel_SQL"
password = "daniel123"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

INGEST_DATE = '2022-07-04'
#INGEST_DATE = sys.argv[1]
ingest_date_list = INGEST_DATE.split('-')
YEAR = int(ingest_date_list[0])
MONTH = int(ingest_date_list[1])
DAY = int(ingest_date_list[2])

sql_query = f"""
        SELECT o.OrderID, o.CustomerID as CustomerID_sk, o.EmployeeID, o.OrderDate, o.RequiredDate, o.ShippedDate, 
                o.ShipVia, o.Freight,
                od.ProductID, od.UnitPrice, od.Quantity, od.Discount,
                c.CustomerID AS CustomerID_bk, c.FirstName AS FirstName_cust, c.LastName AS LastName_cust, 
                c.Address AS Address_cust, 
                c.City AS City_cust, c.PostalCode AS PostalCode_cust, c.Country AS Country_cust, c.Phone AS Phone_cust
                
        FROM NewStoreDB.dbo.Orders AS o
        JOIN NewStoreDB.dbo.OrderDetails AS od ON od.OrderID = o.OrderID
        JOIN NewStoreDB.dbo.Customers AS c ON c.ID = o.CustomerID
        WHERE CONVERT(DATE, OrderDate) = '{INGEST_DATE}'
"""

df = spark.read.format("jdbc") \
    .option("url", mssql_url) \
    .option("driver", driver) \
    .option("user", username) \
    .option("password", password) \
    .option("dbtable", f"({sql_query}) query") \
    .load()

df = df.withColumn('Created', current_timestamp()) \
    .withColumn('Year', lit(YEAR)) \
    .withColumn('Month', lit(MONTH)) \
    .withColumn('Day', lit(DAY))

print(df.show())

df.coalesce(1).write.save('project_scripts/test_output/NewStoreRawData', format='csv', mode='overwrite', header=True,
              partitionBy=['Year', 'Month', 'Day'])
"""
df.coalesce(1).write.save("hdfs://namenode:9000/NewStoreRawData/", format='parquet', mode='overwrite',
                          partitionBy=['Year', 'Month', 'Day'])
"""