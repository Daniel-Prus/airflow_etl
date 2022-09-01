-- create Data Warehouse
USE MASTER;
DROP DATABASE IF EXISTS NewStoreDW;
CREATE DATABASE NewStoreDW;

USE NewStoreDW;

CREATE TABLE NewStoreRawData(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OrderID] [int] NOT NULL,
	[CustomerID_sk] [nvarchar](100) NOT NULL,
	[EmployeeID] [int] NULL,
	[OrderDate] [datetime] NULL,
	[RequiredDate] [datetime] NULL,
	[ShippedDate] [datetime] NULL,
	[ShipVia] [int] NULL,
	[Freight] [decimal](19, 4) NULL,
	[ProductID] [int] NOT NULL,
	[UnitPrice] [decimal](19, 4) NOT NULL,
	[Quantity] [smallint] NOT NULL,
	[Discount] [decimal](19, 4) NOT NULL,
	[CustomerID_bk] [int] NOT NULL,
	[FirstName_cust] [nvarchar](100) NOT NULL,
	[LastName_cust] [nvarchar](100) NOT NULL,
	[Address_cust] [nvarchar](100) NULL,
	[City_cust] [nvarchar](50) NULL,
	[PostalCode_cust] [varchar](10) NULL,
	[Country_cust] [nvarchar](50) NULL,
	[Phone_cust] [varchar](24) NULL,
	[FileDate] DATE NULL,
	[Created] DATETIME DEFAULT CURRENT_TIMESTAMP
CONSTRAINT [PK_NewStoreRawData_ID] PRIMARY KEY CLUSTERED (ID),
CONSTRAINT UK_NewStoreRawData_OrderID_ProductID UNIQUE (OrderID, ProductID)

)



-- staging
CREATE TABLE STG_DimCustomers(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NOT NULL,
	[FirstName] [nvarchar](100) NOT NULL,
	[LastName] [nvarchar](100) NOT NULL,
	[Address] [nvarchar](100) NULL,
	[City] [nvarchar](50) NULL,
	[PostalCode] [varchar](10) NULL,
	[Country] [nvarchar](50) NULL,
	[Phone] [varchar](24) NULL,
	[RowCodeID]  AS hashbytes('md5', concat(CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone)),
	[IngestDate] DATE,
	[Created] DATETIME default CURRENT_TIMESTAMP,
CONSTRAINT [PK_STG_DimCustomers_ID] PRIMARY KEY CLUSTERED (ID)
)

CREATE TABLE STG_FactOrders(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[EmployeeID] [int] NULL,
	[OrderDate] [datetime] NULL,
	[RequiredDate] [datetime] NULL,
	[ShippedDate] [datetime] NULL,
	[ShipVia] [int] NULL,
	[Freight] [decimal](19, 4) NULL,
	[ProductID] [int] NULL,
	[UnitPrice] [decimal](19, 4) NULL,
	[Quantity] [smallint] NULL,
	[Discount] [decimal](19, 4) NULL,
	[Created] DATETIME default CURRENT_TIMESTAMP,
	[Updated] DATETIME,
CONSTRAINT [PK_STG_FactOrders_ID] PRIMARY KEY CLUSTERED (ID),
CONSTRAINT UK_STG_FactOrders_OrderID_ProductID UNIQUE (OrderID, ProductID)
);


-- data warehouse
CREATE TABLE DW_DimCustomers(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NOT NULL,
	[FirstName] [nvarchar](100) NOT NULL,
	[LastName] [nvarchar](100) NOT NULL,
	[Address] [nvarchar](100) NULL,
	[City] [nvarchar](50) NULL,
	[PostalCode] [varchar](10) NULL,
	[Country] [nvarchar](50) NULL,
	[Phone] [varchar](24) NULL,
	[FromDate] Date NULL,
	[ToDate] Date NULL,
	[Flag] BIT,
CONSTRAINT [PK_DW_DimCustomers_ID] PRIMARY KEY CLUSTERED (ID))

CREATE TABLE DW_FactOrders(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[EmployeeID] [int] NULL,
	[OrderDate] [datetime] NULL,
	[RequiredDate] [datetime] NULL,
	[ShippedDate] [datetime] NULL,
	[ShipVia] [int] NULL,
	[Freight] [decimal](19, 4) NULL,
	[ProductID] [int] NULL,
	[UnitPrice] [decimal](19, 4) NULL,
	[Quantity] [smallint] NULL,
	[Discount] [decimal](19, 4) NULL,
	[Created] DATETIME default CURRENT_TIMESTAMP,
	[Updated] DATETIME,
CONSTRAINT [PK_DW_FactOrders_ID] PRIMARY KEY CLUSTERED (ID),
CONSTRAINT UK_DW_FactOrders_OrderID_ProductID UNIQUE (OrderID, ProductID)
)