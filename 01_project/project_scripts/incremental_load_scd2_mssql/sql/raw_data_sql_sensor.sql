SELECT COUNT(*)
FROM NewStoreDW.dbo.NewStoreRawData
WHERE FileDate = '{{ params.ingest_date }}'