DECLARE @InsertDate DATE = '{{ params.ingest_date }}'
MERGE INTO NewStoreDW.dbo.DW_FactOrders AS DST
	USING (SELECT f.OrderID, dm.ID as CustomerID, f.EmployeeID, f.OrderDate, f.RequiredDate, f.ShippedDate, f.ShipVia, f.Freight, f.ProductID, f.UnitPrice, f.Quantity, f.Discount
			FROM NewStoreDW.dbo.STG_FactOrders as f
			JOIN NewStoreDW.dbo.DW_DimCustomers as dm on dm.CustomerID = f.CustomerID AND Flag = 1
			WHERE CONVERT(DATE, OrderDate) = @InsertDate) as SRC
	ON (SRC.OrderID = DST.OrderID AND SRC.ProductID = DST.ProductID )

	-- insert new records
	WHEN NOT MATCHED THEN
	INSERT (OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ProductID, UnitPrice, Quantity, Discount)
	VALUES (SRC.OrderID, SRC.CustomerID, SRC.EmployeeID, SRC.OrderDate, SRC.RequiredDate, SRC.ShippedDate, SRC.ShipVia, SRC.Freight, SRC.ProductID, SRC.UnitPrice, SRC.Quantity, SRC.Discount)

	-- update existing records
	WHEN MATCHED THEN UPDATE SET
		DST.OrderID	= SRC.OrderID,
		DST.CustomerID = SRC.CustomerID,
		DST.EmployeeID = SRC.EmployeeID,
		DST.OrderDate  = SRC.OrderDate,
		DST.RequiredDate = SRC.RequiredDate,
		DST.ShippedDate	= SRC.ShippedDate,
		DST.ShipVia = SRC.ShipVia,
		DST.Freight	= SRC.Freight,
		DST.ProductID =	SRC.ProductID,
		DST.UnitPrice =	SRC.UnitPrice,
		DST.Quantity = SRC.Quantity,
		DST.Discount = SRC.Discount,
        DST.Updated = current_timestamp;