SELECT COUNT(*)
FROM NewStoreDW.dbo.NewStoreRawData
WHERE FileDate = %(date)s