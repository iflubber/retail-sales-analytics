**Project6:**

**Retail Sales Analytics**

**Overview:**

The objective of the project to illustrate retail analytics using an
online retail dataset containing transactions occurring between
01/12/2010 and 09/12/2011 for a UK-based and registered non-store online
retail. This dataset is used to demonstrate an end-to-end retail
analytic use case on the Hadoop Data Platform distribution:

Data ingestion and cleansing using Apache Pig/Hive

SQL on Hadoop using Hive

Analytics and visualization using Hive/SparkSQL/Tableau

**Data set:**

The original Online Retail data set is available to download on the
\[UCI Machine Learning Repository\]
(<https://archive.ics.uci.edu/ml/datasets/Online+Retail>). It has been
converted using Microsoft Excel to a tab delimited file available for
convenience. The fields in the data as follows.

InvoiceNo-integer -Transaction Number

StockCode-character -SKU Code (Product Code)

Description -character -Product Description

Quantity -int-Quantity ordered

InvoiceDate-character -Transaction Data

UnitPrice-float-Price per unit quantity

CustomerID-character -Customer ID

Country -character -Customer location

**Analysis:**

1.Revenue Aggregate By Country for top 5 countries

2.Sales Metrics like NumCustomers, NumTransactions, AvgNumItems,
MinAmtperCustomer, MaxAmtperCustomer, AvgAmtperCustomer,
StdDevAmtperCustomeretc. .. by country for top 5 countries

3.Daily Sales Activity like NumVisits, TotalAmtetc… per POSIX day of the
year

4.Hourly sales Activity like NumVisits, TotalAmtetc… per hour of day

5.Basket size distribution (Note: Basket size = number of items in a
transaction) ( in this questions, we would like to know that, number of
transactions by each basket size i.e. number of transactions with 3
size, number of transactions with 4 size etc.

6.Top 20 Items sold by frequency

7.Customer Lifetime Value distribution by intervals of 1000’s (Customer
Life time Value = total spend by customer in his/her tenure with the
company) (In this question, we would like to calculate how many
customers with CLV between 1-1000, 1000-2000 etc.). Please note that we
don’t want calculate bins manually and it required to create bins
dynamically.

***Solution:***

Let’s begin by uploading the online retail dataset to HDFS.

-- Create the necessary folders in HDFS

\[cloudera@quickstart \~\]\$ hdfs dfs -mkdir /user/cloudera/**retail**

\[cloudera@quickstart \~\]\$ hdfs dfs -mkdir
/user/cloudera/**retail/retailsalesraw**

\[cloudera@quickstart \~\]\$ hdfs dfs -mkdir
/user/cloudera/**retail/retailsalesclean**

\[cloudera@quickstart \~\]\$ hdfs dfs -mkdir
/user/cloudera/**retail/georevenue**

\[cloudera@quickstart \~\]\$ hdfs dfs -mkdir
/user/cloudera/**retail/marketbaskets**

-- upload the raw retail dataset to HDFS

\[cloudera@quickstart \~\]\$ hdfs dfs -put
/home/cloudera/localdata/OnlineRetail.txt
/user/cloudera/retail/retailsalesraw/

\[cloudera@quickstart \~\]\$ hdfs dfs -ls
/user/cloudera/retail/retailsalesraw

Found 1 items

-rw-r--r-- 1 cloudera cloudera 45547637 2018-06-14 21:38
/user/cloudera/retail/retailsalesraw/OnlineRetail.txt

Perform ETL on the raw data using Pig:

-- Loading raw data

grunt&gt; OnlineRetailRawData = LOAD
'/user/cloudera/retail/retailsalesraw/OnlineRetail.txt' using
PigStorage('\\t') AS (InvoiceNo: int, StockCode: chararray, Description:
chararray, Quantity: int, InvoiceDate: chararray, UnitPrice: float,
CustomerID: int, Country: chararray);

-- Cleansing File

grunt&gt; RetailSalesRaw = FILTER OnlineRetailRawData BY NOT
(InvoiceDate matches 'InvoiceDate');

grunt&gt; RetailSalesClean = FOREACH RetailSalesRaw GENERATE InvoiceNo,
StockCode, Description, Quantity, CONCAT(InvoiceDate,':00') as
(InvoiceDate:chararray), UnitPrice, ROUND(UnitPrice \* Quantity \*
100f)/100f AS (TotalPrice: float), CustomerID, Country;

-- Storing Cleansed File into HDFS

grunt&gt; STORE RetailSalesClean INTO
'**/user/cloudera/retail/retailsalesclean**' USING PigStorage ('\\t');

-- Generate Overall Sales Aggregate and Sales for top 10 countries

grunt&gt; GeoGroup = GROUP RetailSalesClean BY Country;

grunt&gt; GeoRevenue = FOREACH GeoGroup GENERATE group,
ROUND(SUM(RetailSalesClean.TotalPrice)) AS TotalRevenueByCountry;

grunt&gt; GeoRevenueOrdered = ORDER GeoRevenue BY TotalRevenueByCountry
DESC;

grunt&gt; Top10GeoRevenue = LIMIT GeoRevenueOrdered 10;

-- Storing Top10GeoRevenue file into HDFS

grunt&gt; STORE Top10GeoRevenue INTO
'**/user/cloudera/retail/georevenue**' USING PigStorage ('\\t');

These files can further be used in Hive to do the desired analysis.

What we did above using Pig, we could have done the same thing in Hive
too by creating a table in Hive and load the raw data directly using the
following:

-- table to read the raw data

hive&gt; CREATE EXTERNAL TABLE IF NOT EXISTS RetailSalesRaw(

&gt; InvoiceNo bigint,

&gt; StockCode string,

&gt; Description string,

&gt; Quantity int,

&gt; InvoiceDate string,

&gt; UnitPrice double,

&gt; CustomerID int,

&gt; Country string)

&gt; ROW FORMAT DELIMITED

&gt; FIELDS TERMINATED BY '\\t'

&gt; STORED AS TEXTFILE

&gt; LOCATION '/user/cloudera/retail/retailsalesraw'

&gt; TBLPROPERTIES ("skip.header.line.count"="1");

OK

Time taken: 0.118 seconds

However, let’s use the cleansed data from Pig and create the table in
Hive.

-- table to read the cleansed data from Pig

hive&gt; CREATE EXTERNAL TABLE IF NOT EXISTS RetailSales(

&gt; InvoiceNo bigint,

&gt; StockCode string,

&gt; Description string,

&gt; Quantity int,

&gt; InvoiceDate timestamp,

&gt; UnitPrice double,

&gt; TotalPrice double,

&gt; CustomerID int,

&gt; Country string)

&gt; ROW FORMAT DELIMITED

&gt; FIELDS TERMINATED BY '\\t'

&gt; LOCATION '/user/cloudera/retail/retailsalesclean';

Now, let’s do the data preparation for Market Basket Analysis using Pig.

-- Loading raw data

grunt&gt; OnlineRetailData = LOAD
'/user/cloudera/retail/retailsalesraw/OnlineRetail.txt' USING
PigStorage('\\t') AS (InvoiceNo: int, StockCode: chararray, Description:
chararray, Quantity: int, InvoiceDate: chararray, UnitPrice: float,
CustomerID: int, Country: chararray);

-- Remove observations with InvoiceNo = null or StockCode = null

grunt&gt; BasketsRawNotNull = FILTER OnlineRetailData BY (InvoiceNo IS
NOT NULL OR StockCode IS NOT NULL);

-- Remove observations with StockCode = DOT, POST, BANK, M or ''

grunt&gt; BasketsRawStockFiltered = FILTER BasketsRawNotNull BY NOT
(StockCode == ' ' OR StockCode == 'DOT' OR StockCode == 'POST' OR
StockCode == 'BANK' OR StockCode == 'M');

-- Extract first 4 digits of stock code and store it in a new variable
StockCat

grunt&gt; BasketsRaw = FOREACH BasketsRawStockFiltered GENERATE
InvoiceNo, SUBSTRING(StockCode,0,4) as (StockCat:chararray);

-- Remove observations with StockCat = 8509

grunt&gt; BasketsRaw1 = FILTER BasketsRaw BY NOT ( StockCat == '8509');

-- Remove duplicates

grunt&gt; BasketsRawU = DISTINCT BasketsRaw1;

-- Remove the baskets with basket size&lt;1 and size&gt;10

grunt&gt; BasketsGroupR1 = GROUP BasketsRawU by InvoiceNo;

grunt&gt; BasketsGroupR2 = FOREACH BasketsGroupR1 GENERATE group as IN,
BasketsRawU as Bkt;

grunt&gt; BasketsGroupR3 = FILTER BasketsGroupR2 BY SIZE(Bkt) &gt; 1 AND
SIZE(Bkt) &lt; 10;

grunt&gt; MarketBaskets = FOREACH BasketsGroupR3 GENERATE
FLATTEN(BagToTuple(Bkt.StockCat));

-- Storing Market Baskets

grunt&gt; STORE MarketBaskets INTO '/user/cloudera/retail/marketbaskets'
USING PigStorage (',');

***Analysis:***

**Retail Sales Analysis Using SparkSQL in Zeppelin Notebook**

We will use the RetailSales table we created in Hive after the data
cleansing done in Pig.

-- Revenue Aggregate By Country for top 5 countries

![](./screenshots/media/image1.tiff){width="6.263888888888889in"
height="2.6020833333333333in"}

-   Clearly United Kingdom is leading in terms of revenue generation

-- Sales Metrics like NumCustomers, NumTransactions, AvgNumItems,
MinAmtperCustomer, MaxAmtperCustomer, AvgAmtperCustomer,
StdDevAmtperCustomeretc. .. by country for top 5 countries

![](./screenshots/media/image2.tiff){width="6.263888888888889in"
height="2.813888888888889in"}

-   Again United Kingdom is leading with the largest customer base and
    the largest number of transactions

-- Daily Sales Activity like NumVisits, TotalAmtetc… per POSIX day of
the year

![](./screenshots/media/image3.tiff){width="6.263888888888889in"
height="2.56875in"}

-   336th day of 2010 had the maximum sales activity with 142 visits

-- Hourly sales Activity like NumVisits, TotalAmtetc… per hour of day

![](./screenshots/media/image4.tiff){width="6.263888888888889in"
height="2.5083333333333333in"}

-   Peak sales activity is between 12pm to 1pm with 3220 visits

-- Basket size distribution (Note: Basket size = number of items in a
transaction) ( in this questions, we would like to know that, number of
transactions by each basket size i.e. number of transactions with 3
size, number of transactions with 4 size etc.

![](./screenshots/media/image5.tiff){width="6.263888888888889in"
height="2.4319444444444445in"}

-   Most customers (3723) mostly bought a single item

-- Top 20 Items sold by frequency

![](./screenshots/media/image6.tiff){width="6.263888888888889in"
height="2.420138888888889in"}

-   ‘White Hanging Heart T-Light Holder’ is the most popular item
    followed by ‘Jumbo Bag Red Retrospot’, ‘Regency Cakestand 3 Tier’
    and ‘Party Bunting’

-- Customer Lifetime Value distribution by intervals of 1000’s (Customer
Life time Value = total spend by customer in his/her tenure with the
company) (In this question, we would like to calculate how many
customers with CLV between 1-1000, 1000-2000 etc.)

![](./screenshots/media/image7.tiff){width="6.263888888888889in"
height="2.513888888888889in"}

-   Most customers (2670) are with CLV between 1-1000

-   765 customers are with CLV between 1000-2000

-   347 customers are with CLV between 2000-3000

**Market Basket Analysis Using Spark MLLib in Zeppelin Notebook**

Let’s now perform Market-Basket Analysis on the market-basket data we
prepared in Pig. Market-Basket Analysis will be done using FP Growth
algorithm available in Spark MLLib.

Model parameters:

Minimum Support = 0.7%

Confidence Level = 80%

Number of partitions = 10

import org.apache.spark.mllib.fpm.FPGrowth

import org.apache.spark.rdd.RDD

import sys.process.\_

val data = sc.textFile("/user/cloudera/retail/marketbaskets")

val transactions: RDD\[Array\[String\]\] = data.map(s =&gt;
s.trim.split(','))

val fpg = new FPGrowth()

.setMinSupport(0.007)

.setNumPartitions(10)

val model = fpg.run(transactions)

model.freqItemsets.collect().foreach { itemset =&gt;

println(itemset.items.mkString("\[", ",", "\]") + ", " + itemset.freq)

}

val minConfidence = 0.8

model.generateAssociationRules(minConfidence).collect().foreach { rule
=&gt;

println(

rule.antecedent.mkString("\[", ",", "\]")

+ " =&gt; " + rule.consequent .mkString("\[", ",", "\]")

+ ", " + rule.confidence)

}

The model generates the association rules.

minConfidence: Double = 0.8

\[2319,2238\] =&gt; \[2320\], 0.8857142857142857

\[2266,2072\] =&gt; \[2238\], 0.8571428571428571

Above are the association rules with confidence level = 80%+

The result says the customers buying items with stock code starting with
2319 and 2238 also buys item with stock code starting with 2320 more
than 88% of the time. By running a query on retailsales we can find out
that items with stock code starting with 2319 are exercise books, note
books, treasure book box, magnetic shopping list, magnetic notepad;
items with stock code starting with 2238 are mainly lunch bags; items
with stock code starting with 2320 are grocery bags. So, a customer
buying a notebook and a lunch bag is 88% or more likely to buy a grocery
bag too.

We can further tweak the model parameters and derive more association
rules.
