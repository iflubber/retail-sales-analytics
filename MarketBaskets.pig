-- Loading raw data
OnlineRetailData = LOAD '/user/cloudera/retail/retailsalesraw/OnlineRetail.txt' USING PigStorage('\t') AS (	InvoiceNo: int, StockCode: chararray, Description: chararray, Quantity: int, InvoiceDate: chararray, UnitPrice: float, CustomerID: int, Country: chararray);

-- Remove observations with InvoiceNo = null or StockCode = null 
BasketsRawNotNull = FILTER OnlineRetailData BY (InvoiceNo IS NOT NULL OR StockCode IS NOT NULL);

-- Remove observations with StockCode = DOT, POST, BANK, M or ''
BasketsRawStockFiltered = FILTER BasketsRawNotNull BY NOT (StockCode == ' ' OR StockCode == 'DOT' OR StockCode == 'POST' OR StockCode == 'BANK' OR StockCode == 'M');

-- Extract first 4 digits of stock code and store it in a new variable StockCat
BasketsRaw = FOREACH BasketsRawStockFiltered GENERATE InvoiceNo, SUBSTRING(StockCode,0,4) as (StockCat:chararray);

-- Remove observations with StockCat = 8509
BasketsRaw1 = FILTER BasketsRaw BY NOT ( StockCat == '8509');

-- Remove duplicates
BasketsRawU = DISTINCT BasketsRaw1;

-- Remove the baskets with basket size<1 and size>10
BasketsGroupR1 = GROUP BasketsRawU BY InvoiceNo;

BasketsGroupR2 = FOREACH BasketsGroupR1 GENERATE group AS IN, BasketsRawU as Bkt;

BasketsGroupR3 = FILTER BasketsGroupR2 BY SIZE(Bkt) > 1 AND SIZE(Bkt) < 10;

MarketBaskets = FOREACH BasketsGroupR3 GENERATE FLATTEN(BagToTuple(Bkt.StockCat));

-- Storing Market Baskets
STORE MarketBaskets INTO '/user/cloudera/retail/marketbaskets' USING PigStorage (',');