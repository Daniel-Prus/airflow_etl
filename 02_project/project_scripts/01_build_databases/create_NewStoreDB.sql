-- Need Northwind DB to run

USE MASTER;
DROP DATABASE IF EXISTS NewStoreDB;
CREATE DATABASE NewStoreDB;
USE NewStoreDB;

-- Customers
CREATE TABLE dbo.Customers (
	ID INT IDENTITY(1,1) NOT NULL,
	CustomerID INT NOT NULL,
	FirstName NVARCHAR(100) NOT NULL,
	LastName NVARCHAR(100) NOT NULL,
	[Address] NVARCHAR(100) NULL,
	City NVARCHAR(50) NULL,
	PostalCode VARCHAR(10) NULL,
	Country NVARCHAR(50) NULL,
	Phone VARCHAR(24) NULL,
	CONSTRAINT [PK_ID] PRIMARY KEY CLUSTERED (ID));

INSERT INTO Customers VALUES
		(221, 'Anna', 'Nowak', 'ul. Jana Pawła II 35/44', 'Warszawa', '02-700', 'Polska', '567888987'),
		(889, 'Laurence', 'Lebihan', '12, rue des Bouchers', 'Marseille', '13008', 'France', '91.24.45.40'),
		(415, 'Martín', 'Sommer', 'C/ Araquil, 67', 'Madrid', '28023', 'Spain', '(91) 555 22 82'),
		(120, 'André', 'Fonseca', 'Av. Brasil, 442', 'Campinas', '04876-786', 'Brazil', '(11) 555-9482'),
		(11, 'Simon', 'Crowther', 'South House 300 Queensbridge', 'London', 'SW7 1RZ', 'UK', '(171) 555-7733'),
		(88, 'Peter', 'Franken', 'Berliner Platz 43', 'München', '80805', 'Germany', '089-0877310'),
		(1023, 'Georg', 'Pipps', 'Geislweg 14', 'Salzburg', '5020', 'Austria', '6562-9722'),
		(221, 'Anna', 'Nowak', 'Wyszyńskiego 13', 'Kraków', '15-008', 'Polska', '567888987'),
		(88, 'Peter', 'Franken', 'Berliner Platz 43', 'München', '80805', 'Germany', '008-26277717'),
		(221, 'Anna', 'Nowak', 'Wyszyńskiego 13', 'Kraków', '15-008', 'Polska', '555142458'),
		(988, 'Matti', 'Karttunen', 'Keskuskatu 45', 'Helsinki', '21240', 'Finland', '90-224 8858'),
		(8, 'Palle', 'Ibsen', 'Smagsloget 45', '?rhus', '8200', 'Denmark', '86 21 32 43');

 -- Categories
 CREATE TABLE [dbo].[Categories](
	[CategoryID] [int] NOT NULL,
	[CategoryName] [nvarchar](15) NOT NULL,
	[Description] [ntext] NULL,
	[Picture] [image] NULL,
 CONSTRAINT [PK_Categories] PRIMARY KEY CLUSTERED (CategoryID));

 INSERT INTO Categories
 SELECT * FROM NORTHWND.dbo.Categories;

-- Employees
CREATE TABLE dbo.Employees (
	[EmployeeID] INT NOT NULL,
	[LastName] [nvarchar](20) NOT NULL,
	[FirstName] [nvarchar](10) NOT NULL,
	[Title] [nvarchar](30) NULL,
	[TitleOfCourtesy] [nvarchar](25) NULL,
	[BirthDate] [datetime] NULL,
	[HireDate] [datetime] NULL,
	[Address] [nvarchar](60) NULL,
	[City] [nvarchar](15) NULL,
	[Region] [nvarchar](15) NULL,
	[PostalCode] [nvarchar](10) NULL,
	[Country] [nvarchar](15) NULL,
	[HomePhone] [nvarchar](24) NULL,
	[Extension] [nvarchar](4) NULL,
	[Photo] [image] NULL,
	[Notes] [ntext] NULL,
	[ReportsTo] [int] NULL,
	[PhotoPath] [nvarchar](255) NULL,
 CONSTRAINT [PK_Employees] PRIMARY KEY CLUSTERED (EmployeeID));

INSERT INTO Employees
SELECT * FROM NORTHWND.dbo.Employees;

 -- Shippers
 CREATE TABLE [dbo].[Shippers](
	[ShipperID] [int] NOT NULL,
	[CompanyName] [nvarchar](40) NOT NULL,
	[Phone] [nvarchar](24) NULL,
 CONSTRAINT [PK_Shippers] PRIMARY KEY CLUSTERED (ShipperID));

 INSERT INTO Shippers
 select * from NORTHWND.dbo.Shippers;

-- Products
CREATE TABLE [dbo].[Products](
	[ProductID] [int] NOT NULL,
	[ProductName] [nvarchar](40) NOT NULL,
	[SupplierID] [int] NULL,
	[CategoryID] [int] NULL,
	[QuantityPerUnit] [nvarchar](20) NULL,
	[UnitPrice] [money] NULL,
	[UnitsInStock] [smallint] NULL,
	[UnitsOnOrder] [smallint] NULL,
	[ReorderLevel] [smallint] NULL,
	[Discontinued] [bit] NOT NULL,
 CONSTRAINT [PK_Products] PRIMARY KEY CLUSTERED (ProductID));

INSERT INTO Products
SELECT * FROM NORTHWND.dbo.Products;

-- Orders
CREATE TABLE dbo.Orders (
	OrderID INT NOT NULL,
	CustomerID INT NULL,
	EmployeeID INT NULL,
	[OrderDate] DATETIME NULL,
	[RequiredDate] DATETIME NULL,
	[ShippedDate] DATETIME NULL,
	[ShipVia] INT NULL,
	[Freight] DECIMAL(19,4) NULL,
	CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED (OrderID),
	CONSTRAINT [FK_Orders_Employees] FOREIGN KEY (EmployeeID) REFERENCES dbo.Employees (EmployeeID),
    CONSTRAINT [FK_Orders_Shippers] FOREIGN KEY (ShipVia) REFERENCES dbo.Shippers (ShipperID));

INSERT INTO dbo.Orders VALUES
    (10248, 1, 5, '2022-07-04T00:00:00.000', '2022-08-01T00:00:00.000', '2022-07-16T00:00:00.000', 3, 32.3800),
    (10249, 3, 6, '2022-07-04T00:00:00.000', '2022-08-16T00:00:00.000', '2022-07-10T00:00:00.000', 1, 11.6100),
    (10250, 6, 4, '2022-07-04T00:00:00.000', '2022-08-05T00:00:00.000', '2022-07-12T00:00:00.000', 2, 65.8300),
    (10251, 12, 3, '2022-07-04T00:00:00.000', '2022-08-05T00:00:00.000', '2022-07-15T00:00:00.000', 1, 41.3400),
    (10252, 2, 4, '2022-07-04T00:00:00.000', '2022-08-06T00:00:00.000', '2022-07-11T00:00:00.000', 2, 51.3000),
    (10253, 7, 3, '2022-07-04T00:00:00.000', '2022-07-24T00:00:00.000', '2022-07-16T00:00:00.000', 2, 58.1700),
    (10254, 3, 5, '2022-07-05T00:00:00.000', '2022-08-08T00:00:00.000', '2022-07-23T00:00:00.000', 2, 22.9800),
    (10255, 8, 9, '2022-07-05T00:00:00.000', '2022-08-09T00:00:00.000', '2022-07-15T00:00:00.000', 3, 148.3300),
    (10256, 11, 3, '2022-07-05T00:00:00.000', '2022-08-12T00:00:00.000', '2022-07-17T00:00:00.000', 2, 13.9700),
    (10257, 4, 4, '2022-07-05T00:00:00.000', '2022-08-13T00:00:00.000', '2022-07-22T00:00:00.000', 3, 81.9100),
    (10258, 5, 1, '2022-07-05T00:00:00.000', '2022-08-14T00:00:00.000', '2022-07-23T00:00:00.000', 1, 140.5100),
    (10259, 9, 4, '2022-07-05T00:00:00.000', '2022-08-15T00:00:00.000', '2022-07-25T00:00:00.000', 3, 3.2500),
    (10260, 12, 4, '2022-07-06T00:00:00.000', '2022-08-16T00:00:00.000', '2022-07-29T00:00:00.000', 1, 55.0900),
    (10261, 2, 4, '2022-07-06T00:00:00.000', '2022-08-16T00:00:00.000', '2022-07-30T00:00:00.000', 2, 3.0500),
    (10262, 9, 8, '2022-07-06T00:00:00.000', '2022-08-19T00:00:00.000', '2022-07-25T00:00:00.000', 3, 48.2900),
    (10263, 10, 9, '2022-07-06T00:00:00.000', '2022-08-20T00:00:00.000', '2022-07-31T00:00:00.000', 3, 146.0600),
    (10264, 7, 6, '2022-07-06T00:00:00.000', '2022-08-21T00:00:00.000', '2022-08-23T00:00:00.000', 3, 3.6700);

-- OrderDetails
CREATE TABLE dbo.OrderDetails (
	[OrderID] [int] NOT NULL,
	[ProductID] [int] NOT NULL,
	[UnitPrice] DECIMAL(19,4) NOT NULL,
	[Quantity] [smallint] NOT NULL,
	[Discount] DECIMAL(19,4) NOT NULL,
	CONSTRAINT [PK_Orders_Details] PRIMARY KEY CLUSTERED (OrderID, ProductID),
	CONSTRAINT [FK_OrderDetails_Products] FOREIGN KEY (ProductID) REFERENCES dbo.Products (ProductID));

INSERT INTO dbo.OrderDetails VALUES
    (10248, 11, 14.0000, 12, 0.0000),
    (10248, 42,9.8000, 10, 0.0000),
    (10248, 72,34.8000, 5, 0.0000),
    (10249, 14,18.6000, 9, 0.0000),
    (10249, 51,42.4000, 40, 0.0000),
    (10250, 41,7.7000, 10, 0.0000),
    (10250, 51,42.4000, 35, 0.1500),
    (10250, 65,16.8000, 15, 0.1500),
    (10251, 22,16.8000, 6, 0.0500),
    (10251, 57,15.6000, 15, 0.0500),
    (10251, 65,16.8000, 20, 0.0000),
    (10252, 20,64.8000, 40,0.0500),
    (10252, 33,2.0000, 25, 0.0500),
    (10252, 60,27.2000, 40, 0.0000),
    (10253, 31,10.0000, 20, 0.0000),
    (10253, 39,14.4000, 42, 0.0000),
    (10253, 49,16.0000, 40, 0.0000),
    (10254, 24,3.6000, 15, 0.1500),
    (10254, 55,19.2000, 21, 0.1500),
    (10254, 74,8.0000, 21, 0.0000),
    (10255, 2,15.2000, 20, 0.0000),
    (10255, 16,13.9000, 35, 0.0000),
    (10255, 36,15.2000, 25, 0.0000),
    (10255, 59,44.0000, 30, 0.0000),
    (10256, 53,26.2000, 15, 0.0000),
    (10256, 77,10.4000, 12, 0.0000),
    (10257, 27,35.1000, 25, 0.0000),
    (10257, 39,14.4000, 6, 0.0000),
    (10257, 77,10.4000, 15, 0.0000),
    (10258, 2,15.2000, 50, 0.2000),
    (10258, 5,17.0000, 65, 0.2000),
    (10258, 32,25.6000, 6, 0.2000),
    (10259, 21,8.0000, 10, 0.0000),
    (10259, 37,20.8000, 1, 0.0000),
    (10260, 41,7.7000, 16, 0.2500),
    (10260, 57,15.6000, 50, 0.0000),
    (10260, 62,39.4000, 15, 0.2500),
    (10260, 70,12.0000, 21, 0.2500),
    (10261, 21,8.0000, 20, 0.0000),
    (10261, 35,14.4000, 20, 0.0000),
    (10262, 5,17.0000, 12, 0.2000),
    (10262, 7,24.0000, 15, 0.0000),
    (10262, 56,30.4000, 2, 0.0000),
    (10263, 16,13.9000, 60, 0.2500),
    (10263, 24,3.6000, 28, 0.0000),
    (10263, 30,20.7000, 60, 0.2500),
    (10263, 74,8.0000, 36, 0.2500),
    (10264, 2,15.2000, 35, 0.0000),
    (10264, 41,7.7000, 25, 0.1500);


