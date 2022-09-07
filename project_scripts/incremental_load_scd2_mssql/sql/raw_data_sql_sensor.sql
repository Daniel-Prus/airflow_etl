SELECT COUNT(*)
FROM NewStoreDW.dbo.NewStoreRawData
WHERE FileDate = '%(ingest_date)s'