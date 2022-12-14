USE [master]
GO
/****** Object:  Database [NewStoreDW]    Script Date: 26.08.2022 12:16:20 ******/
CREATE DATABASE [NewStoreDW]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'NewStoreDW', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\NewStoreDW.mdf' , SIZE = 8192KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'NewStoreDW_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\NewStoreDW_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [NewStoreDW] SET COMPATIBILITY_LEVEL = 130
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [NewStoreDW].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [NewStoreDW] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [NewStoreDW] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [NewStoreDW] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [NewStoreDW] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [NewStoreDW] SET ARITHABORT OFF 
GO
ALTER DATABASE [NewStoreDW] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [NewStoreDW] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [NewStoreDW] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [NewStoreDW] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [NewStoreDW] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [NewStoreDW] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [NewStoreDW] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [NewStoreDW] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [NewStoreDW] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [NewStoreDW] SET  ENABLE_BROKER 
GO
ALTER DATABASE [NewStoreDW] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [NewStoreDW] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [NewStoreDW] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [NewStoreDW] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [NewStoreDW] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [NewStoreDW] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [NewStoreDW] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [NewStoreDW] SET RECOVERY FULL 
GO
ALTER DATABASE [NewStoreDW] SET  MULTI_USER 
GO
ALTER DATABASE [NewStoreDW] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [NewStoreDW] SET DB_CHAINING OFF 
GO
ALTER DATABASE [NewStoreDW] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [NewStoreDW] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [NewStoreDW] SET DELAYED_DURABILITY = DISABLED 
GO
EXEC sys.sp_db_vardecimal_storage_format N'NewStoreDW', N'ON'
GO
ALTER DATABASE [NewStoreDW] SET QUERY_STORE = OFF
GO
USE [NewStoreDW]
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
USE [NewStoreDW]
GO
/****** Object:  Table [dbo].[DW_DimCustomers]    Script Date: 26.08.2022 12:16:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DW_DimCustomers](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NOT NULL,
	[FirstName] [nvarchar](100) NOT NULL,
	[LastName] [nvarchar](100) NOT NULL,
	[Address] [nvarchar](100) NULL,
	[City] [nvarchar](50) NULL,
	[PostalCode] [varchar](10) NULL,
	[Country] [nvarchar](50) NULL,
	[Phone] [varchar](24) NULL,
	[FromDate] [date] NULL,
	[ToDate] [date] NULL,
	[Flag] [bit] NULL,
 CONSTRAINT [PK_DW_DimCustomers_ID] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DW_FactOrders]    Script Date: 26.08.2022 12:16:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DW_FactOrders](
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
	[Created] [datetime] NULL,
	[Updated] [datetime] NULL,
 CONSTRAINT [PK_DW_FactOrders_ID] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[NewStoreRawData]    Script Date: 26.08.2022 12:16:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[NewStoreRawData](
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
	[Created] [datetime] NULL,
 CONSTRAINT [PK_NewStoreRawData_ID] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STG_DimCustomers]    Script Date: 26.08.2022 12:16:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STG_DimCustomers](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NOT NULL,
	[FirstName] [nvarchar](100) NOT NULL,
	[LastName] [nvarchar](100) NOT NULL,
	[Address] [nvarchar](100) NULL,
	[City] [nvarchar](50) NULL,
	[PostalCode] [varchar](10) NULL,
	[Country] [nvarchar](50) NULL,
	[Phone] [varchar](24) NULL,
	[RowCodeID]  AS (hashbytes('md5',concat([CustomerID],[FirstName],[LastName],[Address],[City],[PostalCode],[Country],[Phone]))),
	[InsertDate] [date] NULL,
	[Created] [datetime] NULL,
 CONSTRAINT [PK_STG_DimCustomers_ID] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[STG_FactOrders]    Script Date: 26.08.2022 12:16:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[STG_FactOrders](
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
	[Created] [datetime] NULL,
	[Updated] [datetime] NULL,
 CONSTRAINT [PK_STG_FactOrders_ID] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Index [UK_DW_FactOrders_OrderID_ProductID]    Script Date: 26.08.2022 12:16:20 ******/
ALTER TABLE [dbo].[DW_FactOrders] ADD  CONSTRAINT [UK_DW_FactOrders_OrderID_ProductID] UNIQUE NONCLUSTERED 
(
	[OrderID] ASC,
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [UK_NewStoreRawData_OrderID_ProductID]    Script Date: 26.08.2022 12:16:20 ******/
ALTER TABLE [dbo].[NewStoreRawData] ADD  CONSTRAINT [UK_NewStoreRawData_OrderID_ProductID] UNIQUE NONCLUSTERED 
(
	[OrderID] ASC,
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [UK_STG_FactOrders_OrderID_ProductID]    Script Date: 26.08.2022 12:16:20 ******/
ALTER TABLE [dbo].[STG_FactOrders] ADD  CONSTRAINT [UK_STG_FactOrders_OrderID_ProductID] UNIQUE NONCLUSTERED 
(
	[OrderID] ASC,
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DW_FactOrders] ADD  DEFAULT (getdate()) FOR [Created]
GO
ALTER TABLE [dbo].[NewStoreRawData] ADD  DEFAULT (getdate()) FOR [Created]
GO
ALTER TABLE [dbo].[STG_DimCustomers] ADD  DEFAULT (getdate()) FOR [Created]
GO
ALTER TABLE [dbo].[STG_FactOrders] ADD  DEFAULT (getdate()) FOR [Created]
GO
USE [master]
GO
ALTER DATABASE [NewStoreDW] SET  READ_WRITE 
GO
