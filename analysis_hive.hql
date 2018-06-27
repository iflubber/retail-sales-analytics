-- table to read the raw data
CREATE EXTERNAL TABLE IF NOT EXISTS RetailSalesRaw(
  InvoiceNo bigint,
  StockCode string,
  Description string,
  Quantity int,
  InvoiceDate string,
  UnitPrice double,
  CustomerID int,
  Country string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE  
LOCATION '/user/cloudera/retail/retailsalesraw'
TBLPROPERTIES ("skip.header.line.count"="1");

-- table to read the cleansed data from Pig
CREATE EXTERNAL TABLE IF NOT EXISTS RetailSales(
  InvoiceNo bigint,
  StockCode string,
  Description string,
  Quantity int,
  InvoiceDate timestamp,
  UnitPrice double,
  TotalPrice double,
  CustomerID int,
  Country string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/cloudera/retail/retailsalesclean';