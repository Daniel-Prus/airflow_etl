DROP DATABASE IF EXISTS NewStoreDW CASCADE;

CREATE DATABASE IF NOT EXISTS NewStoreDW;


CREATE TABLE IF NOT EXISTS NewStoreDW.NewStoreRawData (
       OrderID BIGINT
      ,CustomerID_sk INTEGER
      ,EmployeeID INTEGER
      ,OrderDate TIMESTAMP
      ,RequiredDate TIMESTAMP
      ,ShippedDate TIMESTAMP
      ,ShipVia INTEGER
      ,Freight DECIMAL(19,4)
      ,ProductID INTEGER
      ,UnitPrice DECIMAL(19,4)
      ,Quantity SMALLINT
      ,Discount DECIMAL(19,4)
      ,CustomerID_bk INTEGER
      ,FirstName_cust VARCHAR(100)
      ,LastName_cust VARCHAR(100)
      ,Address_cust VARCHAR(100)
      ,City_cust VARCHAR(100)
      ,PostalCode_cust VARCHAR(50)
      ,Country_cust VARCHAR(100)
      ,Phone_cust VARCHAR(100)
      ,Created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT "Daily raw data NewStore"
PARTITIONED BY (Year INTEGER, Month INTEGER, Day INTEGER)
STORED AS PARQUET;

