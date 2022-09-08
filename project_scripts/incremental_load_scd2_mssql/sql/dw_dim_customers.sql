DECLARE @InsertDate DATE = '{{ params.ingest_date }}'
INSERT INTO NewStoreDW.dbo.DW_DimCustomers (CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone, FromDate, ToDate, Flag)
SELECT CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone, FromDate, ToDate, Flag
FROM
	(
	MERGE INTO NewStoreDW.dbo.DW_DimCustomers AS DST
	USING (SELECT * FROM NewStoreDW.dbo.STG_DimCustomers WHERE IngestDate = @InsertDate) AS SRC
	ON (SRC.CustomerID = DST.CustomerID)

	-- insert new records
	WHEN NOT MATCHED THEN
	INSERT (CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone, FromDate, ToDate, Flag)
	VALUES (CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone, CONVERT(DATE, CURRENT_TIMESTAMP), '', 1)

	-- update existing records
	WHEN MATCHED
	AND Flag = 1
	AND SRC.RowCodeID <> HASHBYTES ('MD5', CONCAT(DST.CustomerID, DST.FirstName, DST.LastName, DST.Address, DST.City, DST.PostalCode, DST.Country, DST.Phone))
	THEN UPDATE
	SET DST.Flag = 0, DST.ToDate = DATEADD (DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP))
	OUTPUT SRC.CustomerID, SRC.FirstName, SRC.LastName, SRC.Address, SRC.City, SRC.PostalCode, SRC.Country, SRC.Phone, CONVERT(DATE, CURRENT_TIMESTAMP) as FromDate, '' as ToDate, 1 as Flag,  $ACTION AS MergeAction
	) as MRG
WHERE MRG.MergeAction = 'UPDATE';