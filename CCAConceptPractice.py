#Hadoop and Spark Developer Practice Questions:

#From itversity Practice Questions

Welcome to the CCA Developer Practice exam: During this exam you will be working on Itversity Labs cluster provided for you. This Document included all of the relevant information
In this we are providing 20 questions with no time limit and we will neither evaluate nor proctor the exam. You should attempt to complete each of these questions with in 15 minutes each
Disclaimer: The intention is only to introduce how the environment will be like once you start taking the exam. Make sure to follow and understand the instructions provided while taking the exam. Also the questions are at different difficulty level and need not be inline with the actual questions in the exam. Also we do not guarantee that you will pass the exam after taking this test
Problems
These are the problems loaded on your cluster

@Problem1:
Instructions
Connect to the MySQL database on the itversity labs using sqoop and import all of the data from the orders table into HDFS

Data Description
A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 68883 rows of orders data

MySQL database information:

Installation on the node ms.itversity.com
Database name is retail_db
Username: retail_user
Password: itversity
Table name orders
Output Requirements
Place the customer files in the HDFS directory
/user/`whoami`/problem1/solution/
Replace `whoami` with your OS user name
Use a text format with comma as the columnar delimiter
Load every order record completely

Solution:

#With db port number.
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/srikapardhi/itversity/problem1/solution/


#Removing Port No. has also worked.
sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/srikapardhi/itversity/problem1/solution/

Result : Verification

[srikapardhi@gw02 ~]$ hadoop fs -ls /user/srikapardhi/itversity/problem1/solution
Found 5 items
-rw-r--r--   2 srikapardhi hdfs          0 2018-09-02 23:55 /user/srikapardhi/itversity/problem1/solution/_SUCCESS
-rw-r--r--   2 srikapardhi hdfs     741614 2018-09-02 23:55 /user/srikapardhi/itversity/problem1/solution/part-m-00000
-rw-r--r--   2 srikapardhi hdfs     753022 2018-09-02 23:55 /user/srikapardhi/itversity/problem1/solution/part-m-00001
-rw-r--r--   2 srikapardhi hdfs     752368 2018-09-02 23:55 /user/srikapardhi/itversity/problem1/solution/part-m-00002
-rw-r--r--   2 srikapardhi hdfs     752940 2018-09-02 23:55 /user/srikapardhi/itversity/problem1/solution/part-m-00003


@Problem2 :

Instructions
Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname

Data Description
Data is available in local file system /data/retail_db

retail_db information:

Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma(",")
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Output Requirements
Target Columns: customer_lname, customer_fname
Number of Files: 1
Place the output file in the HDFS directory
/user/`whoami`/problem2/solution/
Replace `whoami` with your OS user name
File format should be text
delimiter is (",")
Compression: Uncompressed


Solution:

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/srikapardhi/itversity/problem2/solution/ \
--num-mappers 1 \
--query "SELECT customer_lname, customer_fname FROM customers LEFT OUTER JOIN orders ON customer_id = order_customer_id WHERE \$CONDITIONS AND order_customer_id IS NULL ORDER BY customer_lname, customer_fname"


Result : Verification

[srikapardhi@gw02 ~]$ hadoop fs -ls /user/srikapardhi/itversity/problem2/solution
Found 2 items
-rw-r--r--   2 srikapardhi hdfs          0 2018-09-03 01:10 /user/srikapardhi/itversity/problem2/solution/_SUCCESS
-rw-r--r--   2 srikapardhi hdfs        372 2018-09-03 01:10 /user/srikapardhi/itversity/problem2/solution/part-m-00000
[srikapardhi@gw02 ~]$


Hadoop : Verification

[srikapardhi@gw02 ~]$ hadoop fs -tail /user/srikapardhi/itversity/problem2/solution/part-m-00000
Bolton,Mary
Ellison,Albert
Green,Carolyn
Greene,Mary
Harrell,Mary
Lewis,Mary
Mueller,Mary
Patel,Matthew
Shaw,Mary
Smith,Amanda
Smith,Ashley
...


Problem3:

Instructions
Get top 3 crime types based on number of incidents in RESIDENCE area using "Location Description"

Data Description
Data is available in HDFS under /public/crime/csv

crime data information:

Structure of data: (ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrst, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate, Y Coordinate, Year, Updated on, Latitude, Longitude, Location)
File format - text file
Delimiter - "," (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
Output Requirements
Output Fields: crime_type, incident_count
Output File Format: JSON
Delimiter: N/A
Compression: No
Place the output file in the HDFS directory
/user/`whoami`/problem3/solution/
Replace `whoami` with your OS user name
End of Problem



@Problem4:

Instructions
Convert NYSE data into parquet

NYSE data Description
Data is available in local file system under /data/NYSE (ls -ltr /data/NYSE)

NYSE Data information:

Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
Output Requirements
Column Names: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
Convert file format to parquet
Place the output file in the HDFS directory
/user/`whoami`/problem4/solution/
Replace `whoami` with your OS user name

@Problem5:

Instructions
Get word count for the input data using space as delimiter (for each word, we need to get how many times it is repeated in the entire input data set)

Data Description
Data is available in HDFS /public/randomtextwriter

word count data information:

Number of executors should be 10
executor memory should be 3 GB
Executor cores should be 20 in total (2 per executor)
Number of output files should be 8
Avro dependency details: groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Output Requirements
Output File format: Avro
Output fields: word, count
Compression: Uncompressed
Place the customer files in the HDFS directory
/user/`whoami`/problem5/solution/
Replace `whoami` with your OS user name

@Problem6:

Instructions
Get total number of orders for each customer where the cutomer_state = 'TX'

Data Description
retail_db data is available in HDFS at /public/retail_db

retail_db data information:

Source directories: /public/retail_db/orders and /public/retail_db/customers
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname, customer_state (8th column) and many more
delimiter: (",")
Output Requirements
Output Fields: customer_fname, customer_lname, order_count
File Format: text
Delimiter: Tab character (\t)
Place the result file in the HDFS directory
/user/`whoami`/problem6/solution/
Replace `whoami` with your OS user name

Problem7:

Instructions
List the names of the Top 5 products by revenue ordered on '2013-07-26'. Revenue is considered only for COMPLETE and CLOSED orders.

Data Description
retail_db data is available in HDFS at /public/retail_db

retail_db data information:

Source directories:
/public/retail_db/orders
/public/retail_db/order_items
/public/retail_db/products
Source delimiter: comma(",")
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - order_itemss - order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price
Source Columns - products - product_id, product_category_id, product_name, product_description, product_price, product_image
Output Requirements
Target Columns: order_date, order_revenue, product_name, product_category_id
Data has to be sorted in descending order by order_revenue
File Format: text
Delimiter: colon (:)
Place the output file in the HDFS directory
/user/`whoami`/problem7/solution/
Replace `whoami` with your OS user name

Problem8:

Instructions
List the order Items where the order_status = 'PENDING PAYMENT' order by order_id

Data Description
Data is available in HDFS location

retail_db data information:

Source directories: /data/retail_db/orders
Source delimiter: comma(",")
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Output Requirements
Target columns: order_id, order_date, order_customer_id, order_status
File Format: orc
Place the output files in the HDFS directory
/user/`whoami`/problem8/solution/
Replace `whoami` with your OS user name

Problem9:

Instructions
Remove header from h1b data

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS location: /public/h1b/h1b_data
First record is the header for the data
Output Requirements
Remove the header from the data and save rest of the data as is
Data should be compressed using snappy algorithm
Place the H1B data in the HDFS directory
/user/`whoami`/problem9/solution/
Replace `whoami` with your OS user name

Problem10:

Instructions
Get number of LCAs filed for each year

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data
Ignore first record which is header of the data
YEAR is 8th field in the data
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: text
Output Fields: YEAR, NUMBER_OF_LCAS
Delimiter: Ascii null "\0"
Place the output files in the HDFS directory
/user/`whoami`/problem10/solution/
Replace `whoami` with your OS user name

Problem11:

Instructions
Get number of LCAs by status for the year 2016

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data
Ignore first record which is header of the data
YEAR is 8th field in the data
STATUS is 2nd field in the data
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: json
Output Field Names: year, status, count
Place the output files in the HDFS directory
/user/`whoami`/problem11/solution/
Replace `whoami` with your OS user name

Problem12:

Instructions
Get top 5 employers for year 2016 where the status is WITHDRAWN or CERTIFIED-WITHDRAWN or DENIED

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data
Ignore first record which is header of the data
YEAR is 7th field in the data
STATUS is 2nd field in the data
EMPLOYER is 3rd field in the data
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: parquet
Output Fields: employer_name, lca_count
Data needs to be in descending order by count
Place the output files in the HDFS directory
/user/`whoami`/problem12/solution/
Replace `whoami` with your OS user name


Problem13:

Instructions
Copy all h1b data from HDFS to Hive table excluding those where year is NA or prevailing_wage is NA

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data_noheader
Fields:
ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
Ignore data where PREVAILING_WAGE is NA or YEAR is NA
PREVAILING_WAGE is 7th field
YEAR is 8th field
Number of records matching criteria: 3002373
Output Requirements
Save it in Hive Database
Create Database: CREATE DATABASE IF NOT EXISTS `whoami`
Switch Database: USE `whoami`
Save data to hive table h1b_data
Create table command:

CREATE TABLE h1b_data (
  ID                 INT,
  CASE_STATUS        STRING,
  EMPLOYER_NAME      STRING,
  SOC_NAME           STRING,
  JOB_TITLE          STRING,
  FULL_TIME_POSITION STRING,
  PREVAILING_WAGE    DOUBLE,
  YEAR               INT,
  WORKSITE           STRING,
  LONGITUDE          STRING,
  LATITUDE           STRING
)

Replace `whoami` with your OS user name

@Problem14:

Instructions
Export h1b data from hdfs to MySQL Database

Data Description
h1b data with ascii character "\001" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data_to_be_exported
Fields:
ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
Number of records: 3002373
Output Requirements
Export data to MySQL Database
MySQL database is running on ms.itversity.com
User: h1b_user
Password: itversity
Database Name: h1b_export
Table Name: h1b_data_`whoami`
Nulls are represented as: NA
After export nulls should not be stored as NA in database. It should be represented as database null
Create table command:

CREATE TABLE h1b_data_`whoami` (
  ID                 INT,
  CASE_STATUS        VARCHAR(50),
  EMPLOYER_NAME      VARCHAR(100),
  SOC_NAME           VARCHAR(100),
  JOB_TITLE          VARCHAR(100),
  FULL_TIME_POSITION VARCHAR(50),
  PREVAILING_WAGE    FLOAT,
  YEAR               INT,
  WORKSITE           VARCHAR(50),
  LONGITUDE          VARCHAR(50),
  LATITUDE           VARCHAR(50));

Replace `whoami` with your OS user name
Above create table command can be run using
Login using mysql -u h1b_user -h ms.itversity.com -p
When prompted enter password itversity
Switch to database using use h1b_export
Run above create table command by replacing `whoami` with your OS user name



Solution:

Verification in Hadoop:

[srikapardhi@gw02 ~]$ hadoop fs -ls /public/h1b/h1b_data_to_be_exported
Found 5 items
drwxr-xr-x   - hdfs hdfs          0 2018-03-25 10:55 /public/h1b/h1b_data_to_be_exported/.hive-staging_hive_2018-03-25_09-45-32_865_7287485550796586353-1
-rw-r--r--   3 hdfs hdfs  124263459 2018-03-25 10:55 /public/h1b/h1b_data_to_be_exported/part-00000
-rw-r--r--   3 hdfs hdfs  124265402 2018-03-25 10:55 /public/h1b/h1b_data_to_be_exported/part-00001
-rw-r--r--   3 hdfs hdfs  124318440 2018-03-25 10:55 /public/h1b/h1b_data_to_be_exported/part-00002
-rw-r--r--   3 hdfs hdfs   83011227 2018-03-25 10:55 /public/h1b/h1b_data_to_be_exported/part-00003
[srikapardhi@gw02 ~]$

Create Table Command:

CREATE TABLE h1b_data_srikapardhi (
  ID                 INT,
  CASE_STATUS        VARCHAR(50),
  EMPLOYER_NAME      VARCHAR(100),
  SOC_NAME           VARCHAR(100),
  JOB_TITLE          VARCHAR(100),
  FULL_TIME_POSITION VARCHAR(50),
  PREVAILING_WAGE    FLOAT,
  YEAR               INT,
  WORKSITE           VARCHAR(50),
  LONGITUDE          VARCHAR(50),
  LATITUDE           VARCHAR(50));


[srikapardhi@gw02 ~]$ mysql -u h1b_user -h ms.itversity.com -p
Enter password:

mysql> use h1b_export;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> CREATE TABLE h1b_data_srikapardhi (
    ->   ID                 INT,
    ->   CASE_STATUS        VARCHAR(50),
    ->   EMPLOYER_NAME      VARCHAR(100),
    ->   SOC_NAME           VARCHAR(100),
    ->   JOB_TITLE          VARCHAR(100),
    ->   FULL_TIME_POSITION VARCHAR(50),
    ->   PREVAILING_WAGE    FLOAT,
    ->   YEAR               INT,
    ->   WORKSITE           VARCHAR(50),
    ->   LONGITUDE          VARCHAR(50),
    ->   LATITUDE           VARCHAR(50));
Query OK, 0 rows affected (0.00 sec)

mysql> select * from h1b_data_srikapardhi;
Empty set (0.00 sec)

mysql> exit;
Bye

sqoop export \
--connect jdbc:mysql://ms.itversity.com/h1b_export \
--username h1b_user \
--password itversity \
--table h1b_data_srikapardhi \
--export-dir /public/h1b/h1b_data_to_be_exported \
--input-fields-terminated-by '\001' \
--input-null-string 'NA'


18/09/03 02:13:50 INFO mapreduce.ExportJobBase: Transferred 435.113 MB in 64.9546 seconds (6.6987 MB/sec)
18/09/03 02:13:50 INFO mapreduce.ExportJobBase: Exported 3002373 records.


Verification:

[srikapardhi@gw02 ~]$ mysql -u h1b_user -h ms.itversity.com -p
Enter password:
mysql> use h1b_export;
Database changed
mysql> select * from h1b_data_srikapardhi limit 10;



@Problem15:

Instructions
Connect to the MySQL database on the itversity labs using sqoop and import data with case_status as CERTIFIED

Data Description
A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 3002373 rows of h1b data

MySQL database information:

Installation on the node ms.itversity.com
Database name is h1b_db
Username: h1b_user
Password: itversity
Table name h1b_data
Output Requirements
Place the h1b related data in files in HDFS directory
/user/`whoami`/problem15/solution/
Replace `whoami` with your OS user name
Use avro file format
Load only those records which have case_status as CERTIFIED completely
There are 2615623 such records

Solution:

sqoop import \
--connect jdbc:mysql://ms.itversity.com/h1b_db \
--username h1b_user \
--password itversity \
--table h1b_data \
--where "case_status = 'CERTIFIED'" \
--target-dir /user/srikapardhi/itversity/problem15/solution/ \
--as-avrodatafile

Delete Wrong path created earlier.
[srikapardhi@gw02 ~]$ hadoop fs -rm -r /user/srikapardhi/problem15
18/09/03 02:40:27 INFO fs.TrashPolicyDefault: Moved: 'hdfs://nn01.itversity.com:8020/user/srikapardhi/problem15' to trash at: hdfs://nn01.itversity.com:8020/user/srikapardhi/.Trash/Current/user/srikapardhi/problem15
[srikapardhi@gw02 ~]$ hadoop fs -ls
Found 3 items
drwx------   - srikapardhi hdfs          0 2018-09-03 02:40 .Trash
drwx------   - srikapardhi hdfs          0 2018-09-03 02:25 .staging
drwxr-xr-x   - srikapardhi hdfs          0 2018-09-03 01:09 itversity

Verification:

[srikapardhi@gw02 ~]$ hadoop fs -ls /user/srikapardhi/itversity/problem15/solution
Found 5 items
-rw-r--r--   2 srikapardhi hdfs          0 2018-09-03 02:41 /user/srikapardhi/itversity/problem15/solution/_SUCCESS
-rw-r--r--   2 srikapardhi hdfs   96323828 2018-09-03 02:41 /user/srikapardhi/itversity/problem15/solution/part-m-00000.avro
-rw-r--r--   2 srikapardhi hdfs  100310277 2018-09-03 02:41 /user/srikapardhi/itversity/problem15/solution/part-m-00001.avro
-rw-r--r--   2 srikapardhi hdfs  100277022 2018-09-03 02:41 /user/srikapardhi/itversity/problem15/solution/part-m-00002.avro
-rw-r--r--   2 srikapardhi hdfs   97589863 2018-09-03 02:41 /user/srikapardhi/itversity/problem15/solution/part-m-00003.avro


Problem16:

Instructions
Get NYSE data in ascending order by date and descending order by volume

Data Description
NYSE data with "," as delimiter is available in HDFS

NYSE data information:

HDFS location: /public/nyse
There is no header in the data
Output Requirements
Save data back to HDFS
Column order: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
File Format: text
Delimiter: :
Place the sorted NYSE data in the HDFS directory
/user/`whoami`/problem16/solution/
Replace `whoami` with your OS user name

Problem17:

Instructions
Get the stock tickers from NYSE data for which full name is missing in NYSE symbols data

Data Description
NYSE data with "," as delimiter is available in HDFS

NYSE data information:

HDFS location: /public/nyse
There is no header in the data
NYSE Symbols data with " " as delimiter is available in HDFS

NYSE Symbols data information:

HDFS location: /public/nyse_symbols
First line is header and it should be included
Output Requirements
Get unique stock ticker for which corresponding names are missing in NYSE symbols data
Save data back to HDFS
File Format: avro
Avro dependency details:
groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Place the sorted NYSE data in the HDFS directory
/user/`whoami`/problem17/solution/
Replace `whoami` with your OS user name


Problem18:

Instructions
Get the name of stocks displayed along with other information

Data Description
NYSE data with "," as delimiter is available in HDFS

NYSE data information:

HDFS location: /public/nyse
There is no header in the data
NYSE Symbols data with tab character (\t) as delimiter is available in HDFS

NYSE Symbols data information:

HDFS location: /public/nyse_symbols
First line is header and it should be included
Output Requirements
Get all NYSE details along with stock name if exists, if not stockname should be empty
Column Order: stockticker, stockname, transactiondate, openprice, highprice, lowprice, closeprice, volume
Delimiter: ,
File Format: text
Place the data in the HDFS directory
/user/`whoami`/problem18/solution/
Replace `whoami` with your OS user name


Problem19:

Instructions
Get number of companies who filed LCAs for each year

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data_noheader
Fields:
ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
Use EMPLOYER_NAME as the criteria to identify the company name to get number of companies
YEAR is 8th field
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: text
Delimiter: tab character "\t"
Output Field Order: year, lca_count
Place the output files in the HDFS directory
/user/`whoami`/problem19/solution/
Replace `whoami` with your OS user name



Problem20:

Instructions
Connect to the MySQL database on the itversity labs using sqoop and import data with employer_name, case_status and count. Make sure data is sorted by employer_name in ascending order and by count in descending order

Data Description
A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 3002373 rows of h1b data

MySQL database information:

Installation on the node ms.itversity.com
Database name is h1b_db
Username: h1b_user
Password: itversity
Table name h1b_data
Output Requirements
Place the h1b related data in files in HDFS directory
/user/`whoami`/problem20/solution/
Replace `whoami` with your OS user name
Use text file format and tab (\t) as delimiter
Hint: You can use Spark with JDBC or Sqoop import with query
You might not get such hints in actual exam
Output should contain employer name, case status and count

Solution:

Verification of query/access:

sqoop eval \
--connect jdbc:mysql://ms.itversity.com/h1b_db \
--username h1b_user \
--password itversity \
--query "SELECT count(*) FROM (SELECT EMPLOYER_NAME, CASE_STATUS, count(1) AS COUNT FROM h1b_data GROUP BY EMPLOYER_NAME, CASE_STATUS) DBQUERY"

------------------------
| count(*)             |
------------------------
| 332714               |
------------------------

#Notes : The property "Dorg.apache.sqoop.splitter.allow_text_splitter=true" is required when you are using --split-by is used on a column which is of text type.
#Will fail as order_status is a non numeric field. Splitting on string field is not straight forward.
#So, need to use property.


sqoop import \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect  jdbc:mysql://ms.itversity.com/h1b_db \
--username h1b_user \
--password itversity \
--query "SELECT EMPLOYER_NAME, CASE_STATUS, count(1) AS COUNT FROM h1b_data WHERE \$CONDITIONS GROUP BY EMPLOYER_NAME, CASE_STATUS ORDER BY EMPLOYER_NAME, COUNT DESC" \
--target-dir /user/srikapardhi/itversity/problem20/solution/ \
--as-textfile \
--fields-terminated-by '\t' \
--split-by case_status

#When using query, warehouse cannot be used, we need to use target-dir and split-by needs to given as the tool doesn't know how to divide the data between mappers if there are more than 2 mappers.

Verification:

[srikapardhi@gw02 ~]$ hadoop fs -ls /user/srikapardhi/itversity/problem20/solution
Found 7 items
-rw-r--r--   2 srikapardhi hdfs          0 2018-09-03 03:35 /user/srikapardhi/itversity/problem20/solution/_SUCCESS
-rw-r--r--   2 srikapardhi hdfs          0 2018-09-03 03:34 /user/srikapardhi/itversity/problem20/solution/part-m-00000
-rw-r--r--   2 srikapardhi hdfs   11481554 2018-09-03 03:35 /user/srikapardhi/itversity/problem20/solution/part-m-00001
-rw-r--r--   2 srikapardhi hdfs         28 2018-09-03 03:34 /user/srikapardhi/itversity/problem20/solution/part-m-00002
-rw-r--r--   2 srikapardhi hdfs        773 2018-09-03 03:34 /user/srikapardhi/itversity/problem20/solution/part-m-00003
-rw-r--r--   2 srikapardhi hdfs          0 2018-09-03 03:34 /user/srikapardhi/itversity/problem20/solution/part-m-00004
-rw-r--r--   2 srikapardhi hdfs    1053631 2018-09-03 03:34 /user/srikapardhi/itversity/problem20/solution/part-m-00005


[srikapardhi@gw02 ~]$ hadoop fs -tail /user/srikapardhi/itversity/problem20/solution/part-m-00001
AL CORP.	CERTIFIED	9
ZYNP INTERNATIONAL CORP.	CERTIFIED-WITHDRAWN	1
ZYNX HEALTH, INC.	CERTIFIED	3
ZYNX HEALTH, INC.	CERTIFIED-WITHDRAWN	3


#ArunBlog Questions and Solutions in python.

Problem 1:
1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes.
4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
5.Store the result as parquet file into hdfs using gzip compression under folder
/user/cloudera/problem1/result4a-gzip
/user/cloudera/problem1/result4b-gzip
/user/cloudera/problem1/result4c-gzip
6.Store the result as parquet file into hdfs using snappy compression under folder
/user/cloudera/problem1/result4a-snappy
/user/cloudera/problem1/result4b-snappy
/user/cloudera/problem1/result4c-snappy
7.Store the result as CSV file into hdfs using No compression under folder
/user/cloudera/problem1/result4a-csv
/user/cloudera/problem1/result4b-csv
/user/cloudera/problem1/result4c-csv
8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result



Problem 2:
1.Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. columns should be delimited by pipe '|'
2.move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder
3.Change permissions of all the files under /user/cloudera/problem2/products such that owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions
4.read data in /user/cloudera/problem2/products and do the following operations using a) dataframes api b) spark sql c) RDDs aggregateByKey method. Your solution should have three sets of steps. Sort the resultant dataset by category id
filter such that your RDD\DF has products whose price is lesser than 100 USD
on the filtered data set find out the higest value in the product_price column under each category
on the filtered data set also find out total products under each category
on the filtered data set also find out the average price of the product under each category
on the filtered data set also find out the minimum price of the product under each category
5.store the result in avro file using snappy compression under these folders respectively
/user/cloudera/problem2/products/result-df
/user/cloudera/problem2/products/result-sql
/user/cloudera/problem2/products/result-rdd


Problem 3: Perform in the same sequence

1.Import all tables from mysql database into hdfs as avro data files. use compression and the compression codec should be snappy. data warehouse directory should be retail_stage.db
2.Create a metastore table that should point to the orders data imported by sqoop job above. Name the table orders_sqoop.
3.Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop.
4.query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from order_sqoop.
5.Now create a table named retail.orders_avro in hive stored as avro, the table should have same table definition as order_sqoop. Additionally, this new table should be partitioned by the order month i.e -> year-order_month.(example: 2014-01)
6.Load data into orders_avro table from orders_sqoop table.
7.Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_avro
8.evolve the avro schema related to orders_sqoop table by adding more fields named (order_style String, order_zone Integer)
9.insert two more records into orders_sqoop table.
10.Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
11.query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop



In this problem, we will focus on conversion between different file formats using spark or hive. This is a very import examination topic. I recommend that you master the data file conversion techniques and understand the limitations. You should have an alternate method of accomplishing a solution to the problem in case your primary method fails for any unknown reason. For example, if saving the result as a text file with snappy compression fails while using spark then you should be able to accomplish the same thing using Hive. In this blog\video I am going to walk you through some scenarios that cover alternative ways of dealing with same problem.

Problem 4:
1.Import orders table from mysql as text file to the destination /user/cloudera/problem5/text. Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n").
2.Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
3.Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
4.Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
5.Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
6.Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
7.Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
8.Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc




Sqoop is one of the important topics for the exam. Based on generally reported exam pattern from anonymous internet bloggers, you can expect 2 out of 10 questions on this topic related to Data Ingest and Data Export using Sqoop. Hence, 20% of the exam score can be obtained just by practicing simple Sqoop concepts. Sqoop can be mastered easily (i.e in a few hours) at the skill level that CCA 175 exam is expecting you to demonstrate. I created this problem focusing on Sqoop alone, if you are able to perform this exercise on your own or at worst using just the sqoop user guide then there is a very very high chance that you can score the 20% easily.

Pre-Work: Please perform these steps before solving the problem
1. Login to MySQL using below commands on a fresh terminal window
    mysql -u retail_dba -p
    Password = cloudera
2. Create a replica product table and name it products_replica
    create table products_replica as select * from products
3. Add primary key to the newly created table
    alter table products_replica add primary key (product_id);
4. Add two more columns
    alter table products_replica add column (product_grade int, product_sentiment varchar(100))
5. Run below two update statements to modify the data
    update products_replica set product_grade = 1  where product_price > 500;
    update products_replica set product_sentiment  = 'WEAK'  where product_price between 300 and  500;

Problem 5: Above steps are important so please complete them successfully before attempting to solve the problem
1.Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '|' and lines are separated by '\n'. Null values are represented as -1 for numbers and "NOT-AVAILABLE" for strings. Only records with product id greater than or equal to 1 and less than or equal to 1000 should be imported and use 3 mappers for importing. The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text.
2.Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '*' and lines are separated by '\n'. Null values are represented as -1000 for numbers and "NA" for strings. Only records with product id less than or equal to 1111 should be imported and use 2 mappers for importing. The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text-part1.
3.Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '*' and lines are separated by '\n'. Null values are represented as -1000 for numbers and "NA" for strings. Only records with product id greater than 1111 should be imported and use 5 mappers for importing. The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text-part2.
4.Using sqoop merge data available in /user/cloudera/problem5/products-text-part1 and /user/cloudera/problem5/products-text-part2 to produce a new set of files in /user/cloudera/problem5/products-text-both-parts
5.Using sqoop do the following. Read the entire steps before you create the sqoop job.
create a sqoop job Import Products_replica table as text file to directory /user/cloudera/problem5/products-incremental. Import all the records.
insert three more records to Products_replica from mysql
run the sqoop job again so that only newly added records can be pulled from mysql
insert 2 more records to Products_replica from mysql
run the sqoop job again so that only newly added records can be pulled from mysql
Validate to make sure the records have not be duplicated in HDFS
6.Using sqoop do the following. Read the entire steps before you create the sqoop job.
create a hive table in database named problem5 using below command
create table products_hive  (product_id int, product_category_id int, product_name string, product_description string, product_price float, product_imaage string,product_grade int,  product_sentiment string);
create a sqoop job Import Products_replica table as hive table to database named problem5. name the table as products_hive.
insert three more records to Products_replica from mysql
run the sqoop job again so that only newly added records can be pulled from mysql
insert 2 more records to Products_replica from mysql
run the sqoop job again so that only newly added records can be pulled from mysql
Validate to make sure the records have not been duplicated in Hive table
7.Using sqoop do the following. .
insert 2 more records into products_hive table using hive.
create table in mysql using below command
create table products_external  (product_id int(11) primary Key, product_grade int(11), product_category_id int(11), product_name varchar(100), product_description varchar(100), product_price float, product_impage varchar(500), product_sentiment varchar(100));
export data from products_hive (hive) table to (mysql) products_external table.
insert 2 more records to Products_hive table from hive
export data from products_hive table to products_external table.
Validate to make sure the records have not be duplicated in mysql table



Problem 6: Provide two solutions for steps 2 to 7
Using HIVE QL over Hive Context
Using Spark SQL over Spark SQL Context or by using RDDs
1.create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
2.On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
3.Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
4.find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
5.On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
6.On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
7.Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]




This question focusses on validating your flume skills. You can either learn flume by following the video accompanied with this post or learn flume elsewhere and then solve this problem while using the video as a reference. This video serves both as tutorial and walkthrough of how to leverage flume for data ingestion.

Note: While this post only provides specifics related to solving the problem, the video provides an introduction, explanation and more importantly application of flume knowledge.


Problem 7:
1.This step comprises of three substeps. Please perform tasks under each subset completely
using sqoop pull data from MYSQL orders table into /user/cloudera/problem7/prework as AVRO data file using only one mapper
Pull the file from \user\cloudera\problem7\prework into a local folder named flume-avro
create a flume agent configuration such that it has an avro source at localhost and port number 11112,  a jdbc channel and an hdfs file sink at /user/cloudera/problem7/sink
Use the following command to run an avro client flume-ng avro-client -H localhost -p 11112 -F <<Provide your avro file path here>>
2.The CDH comes prepackaged with a log generating job. start_logs, stop_logs and tail_logs. Using these as an aid and provide a solution to below problem. The generated logs can be found at path /opt/gen_logs/logs/access.log
run start_logs
write a flume configuration such that the logs generated by start_logs are dumped into HDFS at location /user/cloudera/problem7/step2. The channel should be non-durable and hence fastest in nature. The channel should be able to hold a maximum of 1000 messages and should commit after every 200 messages.
Run the agent.
confirm if logs are getting dumped to hdfs.
run stop_logs.
