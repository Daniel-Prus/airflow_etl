from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Get_rawdata_mssql").config(
    "spark.jars", "project_scripts/lib/enu/mssql-jdbc-11.2.0.jre8.jar").getOrCreate()

# spark-submit --jars project_scripts/lib/enu/mssql-jdbc-11.2.0.jre8.jar project_scripts/01_mssql.py

mssql_url = "jdbc:sqlserver://localhost:1433;databaseName=NewStoreDB;encrypt=true;trustServerCertificate=true;"
username = "Daniel_SQL"
password = "daniel123"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

sql_query = """




"""

df = spark.read.format("jdbc") \
    .option("url", mssql_url) \
    .option("driver", driver) \
    .option("user", username) \
    .option("password", password) \
    .option("dbtable", "(SELECT * FROM NewStoreDB.dbo.Orders) query") \
    .load()

print(df.show())
print("Job complete.")