DECLARE @IngestDate DATE ='{{params.ingest_date}}'
INSERT INTO  NewStoreDW.dbo.NewStoreRawData
            (OrderID, CustomerID_sk, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight,
			 ProductID, UnitPrice, Quantity, Discount, CustomerID_bk, FirstName_cust, LastName_cust,
			Address_cust, City_cust, PostalCode_cust, Country_cust, Phone_cust, FileDate)

SELECT o.OrderID, o.CustomerID, o.EmployeeID, o.OrderDate, o.RequiredDate, o.ShippedDate, o.ShipVia, o.Freight,
	    od.ProductID, od.UnitPrice, od.Quantity, od.Discount,
	    c.CustomerID, c.FirstName, c.LastName, c.Address, c.City, c.PostalCode, c.Country, c.Phone, @IngestDate
FROM NewStoreDB.dbo.Orders AS o
JOIN NewStoreDB.dbo.OrderDetails AS od ON od.OrderID = o.OrderID
JOIN NewStoreDB.dbo.Customers AS c ON c.ID = o.CustomerID
WHERE CONVERT(DATE, OrderDate) = @IngestDate;
