class MsSqlQuerySupportSCD2:
    raw_data_sensor_query = """SELECT OrderID FROM NewStoreDW.dbo.NewStoreRawData WHERE FileDate='{{ ds }}' """

    def __init__(self, ingest_date: str = None, source_db: str = None, destination_db: str = None,
                 source_table: str = None, destination_table: str = None) -> None:
        self.ingest_date = ingest_date
        self.source_db = source_db
        self.destination_db = destination_db
        self.source_table = source_table
        self.destination_table = destination_table

    def load_raw_data_query(self) -> str:
        sql = """
        DECLARE @IngestDate DATE = '{ingest_date}'
        INSERT INTO  {destination_db}.{destination_table}
                (OrderID, CustomerID_sk, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight,
                 ProductID, UnitPrice, Quantity, Discount, CustomerID_bk, FirstName_cust, LastName_cust,
                Address_cust, City_cust, PostalCode_cust, Country_cust, Phone_cust, FileDate)

        SELECT o.OrderID, o.CustomerID, o.EmployeeID, o.OrderDate, o.RequiredDate, o.ShippedDate, o.ShipVia, o.Freight,
                od.ProductID, od.UnitPrice, od.Quantity, od.Discount,
                c.CustomerID, c.FirstName, c.LastName, c.Address, c.City, c.PostalCode, c.Country, c.Phone, @IngestDate
        FROM {source_db}.{source_table} AS o
        JOIN {source_db}.dbo.OrderDetails AS od ON od.OrderID = o.OrderID
        JOIN {source_db}.dbo.Customers AS c ON c.ID = o.CustomerID
        WHERE CONVERT(DATE, OrderDate) = @IngestDate;
        """.format(ingest_date=self.ingest_date, destination_db=self.destination_db, source_db=self.source_db,
                   destination_table=self.destination_table, source_table=self.source_table)
        return sql

    def load_staging_dim_customers_query(self) -> str:
        sql = """
        DECLARE @IngestDate DATE ='{ingest_date}'
        INSERT INTO {destination_db}.{destination_table} 
                    (CustomerID, FirstName, LastName, Address, City, PostalCode, Country, Phone, IngestDate)
        SELECT DISTINCT SRC.CustomerID_bk, SRC.FirstName_cust, SRC.LastName_cust, SRC.Address_cust, SRC.City_cust, 
                        SRC.PostalCode_cust, SRC.Country_cust, SRC.Phone_cust,  @IngestDate as IngestDate
        FROM {source_db}.{source_table} AS SRC
        WHERE HASHBYTES ('MD5', CONCAT(SRC.CustomerID_bk, SRC.FirstName_cust, SRC.LastName_cust, SRC.Address_cust, 
                        SRC.City_cust, SRC.PostalCode_cust, SRC.Country_cust, SRC.Phone_cust))
                NOT IN (SELECT DST.RowCodeID  FROM {destination_db}.{destination_table} as DST)
                AND CONVERT(DATE, OrderDate) = @IngestDate
        """.format(ingest_date=self.ingest_date, destination_db=self.destination_db, source_db=self.source_db,
                   destination_table=self.destination_table, source_table=self.source_table)
        return sql

    def get_last_row_sql_sensor_query(self) -> str:
        sql = """SELECT TOP 1 * FROM {destination_db}.{destination_table} WHERE 
            FileDate = '{ingest_date}' ORDER BY ID DESC;""".format(destination_db=self.destination_db,
                                                                   destination_table=self.destination_table,
                                                                   ingest_date=self.ingest_date)
        return sql

    def get_rows_count_sql_sensor_query(self) -> str:
        sql = """SELECT COUNT(*) FROM {destination_db}.{destination_table} WHERE 
                    FileDate = '{ingest_date}';""".format(destination_db=self.destination_db,
                                                          destination_table=self.destination_table,
                                                          ingest_date=self.ingest_date)
        return sql

