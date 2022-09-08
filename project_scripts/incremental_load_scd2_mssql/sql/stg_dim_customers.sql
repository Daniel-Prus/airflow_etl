DECLARE @IngestDate DATE = '{{ params.ingest_date }}'
        INSERT INTO NewStoreDW.dbo.STG_DimCustomers
                    (CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone, IngestDate)
        SELECT DISTINCT SRC.CustomerID_bk, SRC.FirstName_cust, SRC.LastName_cust, SRC.Address_cust, SRC.City_cust,
                        SRC.PostalCode_cust, SRC.Country_cust, SRC.Phone_cust,  @IngestDate as IngestDate
        FROM NewStoreDW.dbo.NewStoreRawData AS SRC
        WHERE HASHBYTES ('MD5', CONCAT(SRC.CustomerID_bk, SRC.FirstName_cust, SRC.LastName_cust, SRC.Address_cust,
                        SRC.City_cust, SRC.PostalCode_cust, SRC.Country_cust, SRC.Phone_cust))
                NOT IN (SELECT DST.RowCodeID  FROM NewStoreDW.dbo.STG_DimCustomers as DST)
                AND CONVERT(DATE, OrderDate) = @IngestDate