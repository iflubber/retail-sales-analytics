-- Loading raw data
OnlineRetailRawData = LOAD '/user/cloudera/retail/retailsalesraw/OnlineRetail.txt' using PigStorage('\t') AS (InvoiceNo: int, StockCode: chararray, Description: chararray, Quantity: int, InvoiceDate: chararray, UnitPrice: float, CustomerID: int, Country: chararray);

-- Cleansing File                        
RetailSalesRaw = FILTER OnlineRetailRawData BY NOT (InvoiceDate matches 'InvoiceDate');
RetailSalesClean = FOREACH RetailSalesRaw GENERATE InvoiceNo, StockCode, Description, Quantity, CONCAT(InvoiceDate,':00') as (InvoiceDate:chararray), UnitPrice, ROUND(UnitPrice * Quantity * 100f)/100f AS (TotalPrice: float), CustomerID, Country;

-- Storing Cleansed File                                                    
STORE RetailSalesClean INTO '/user/cloudera/retail/retailsalesclean' USING PigStorage ('\t');

-- Generate Overall Sales Aggregate and Sales for top 10 countries
GeoGroup = GROUP RetailSalesClean BY Country;
GeoRevenue  = FOREACH GeoGroup GENERATE group, ROUND(SUM(RetailSalesClean.TotalPrice)) AS TotalRevenueByCountry;
GeoRevenueOrdered = ORDER GeoRevenue BY TotalRevenueByCountry DESC;
Top10GeoRevenue = LIMIT GeoRevenueOrdered 10;

STORE Top10GeoRevenue INTO '/user/cloudera/retail/georevenue' USING PigStorage ('\t');