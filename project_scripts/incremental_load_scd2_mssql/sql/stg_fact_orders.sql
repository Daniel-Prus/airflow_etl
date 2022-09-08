DECLARE @InsertDate DATE = '{{ params.ingest_date }}'
MERGE INTO NewStoreDW.dbo.stg_FactOrders AS DST
	USING (SELECT * FROM NewStoreDW.dbo.NewStoreRawData WHERE CONVERT(DATE, OrderDate) = @InsertDate) as SRC
	ON (SRC.OrderID = DST.OrderID AND SRC.ProductID = DST.ProductID )

	-- insert new records
	WHEN NOT MATCHED THEN
	INSERT (OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ProductID, UnitPrice, Quantity, Discount)
	VALUES (SRC.OrderID, SRC.CustomerID_bk, SRC.EmployeeID, SRC.OrderDate, SRC.RequiredDate, SRC.ShippedDate, SRC.ShipVia, SRC.Freight, SRC.ProductID, SRC.UnitPrice, SRC.Quantity, SRC.Discount)

	-- update existing records
	WHEN MATCHED THEN UPDATE SET
		DST.OrderID	= SRC.OrderID,
		DST.CustomerID = SRC.CustomerID_bk,
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