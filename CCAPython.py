#CCA175 : CCA Spark and Hadoop Developer Certification Exam 
Number of Questions: 8 or 10–12 performance-based (hands-on) tasks on Cloudera Enterprise cluster. 
Time Limit: 120 minutes
Passing Score: 70%
Language: English
Price: USD $295

#Data Ingest
#The skills to transfer data between external systems and your cluster. This includes the following:
1.Import data from a MySQL database into HDFS using Sqoop
2.Export data to a MySQL database from HDFS using Sqoop
3.Change the delimiter and file format of data during import using Sqoop
4.Ingest real-time and near-real-time streaming data into HDFS
5.Process streaming data as it is loaded onto the cluster
6.Load data into and out of HDFS using the Hadoop File System commands
#Transform, Stage, and Store
#Convert a set of data values in a given format stored in HDFS into new data values or a new data format and write them into HDFS.
1.Load RDD data from HDFS for use in Spark applications
2.Write the results from an RDD back into HDFS using Spark
3.Read and write files in a variety of file formats
4.Perform standard extract, transform, load (ETL) processes on data
#Data Analysis
#Use Spark SQL to interact with the metastore programmatically in your applications. 
# Generate reports by using queries against loaded data.
1.Use metastore tables as an input source or an output sink for Spark applications
2.Understand the fundamentals of querying datasets in Spark
3.Filter data using Spark
4.Write queries that calculate aggregate statistics
5.Join disparate datasets using Spark
6.Produce ranked or sorted data
#Configuration
This is a practical exam and the candidate should be familiar with all aspects of generating a result, not just writing code.
1.Supply command-line options to change your application configuration, such as increasing available memory


#Technologies Mapped against Required Skills 
#Data Ingest
#The skills to transfer data between external systems and your cluster. This includes the following:
1.Import data from a MySQL database into HDFS using Sqoop : #Sqoop
2.Export data to a MySQL database from HDFS using Sqoop : #Sqoop
3.Change the delimiter and file format of data during import using Sqoop : #Sqoop
4.Ingest real-time and near-real-time streaming data into HDFS : #Flume or Spark Streaming
5.Process streaming data as it is loaded onto the cluster : #Flume or Spark Streaming
6.Load data into and out of HDFS using the Hadoop File System commands : #HDFS Command Line
#Transform, Stage, and Store
#Convert a set of data values in a given format stored in HDFS into new data values or a new data format and write them into HDFS.
1.Load RDD data from HDFS for use in Spark applications : #Spark RDD and Spark DF
2.Write the results from an RDD back into HDFS using Spark : #Spark RDD and Spark DF
3.Read and write files in a variety of file formats : #Spark RDD and Spark DF
4.Perform standard extract, transform, load (ETL) processes on data : #Spark RDD, Spark DF and Hive
#Data Analysis
#Use Spark SQL to interact with the metastore programmatically in your applications. 
# Generate reports by using queries against loaded data.
1.Use metastore tables as an input source or an output sink for Spark applications : #Spark RDD, Spark DF,  Spark SQL, Hive, Impala
2.Understand the fundamentals of querying datasets in Spark : #Spark RDD, Spark DF,  Spark SQL
3.Filter data using Spark : #Spark RDD, Spark DF,  Spark SQL
4.Write queries that calculate aggregate statistics : #Spark DF,  Spark SQL, Hive and Impala
5.Join disparate datasets using Spark : #Spark RDD, Spark DF and Spark SQL
6.Produce ranked or sorted data:  # Spark RDD, Spark DF,  Spark SQL, Hive and Impala
#Configuration
This is a practical exam and the candidate should be familiar with all aspects of generating a result, not just writing code.
1.Supply command-line options to change your application configuration, such as increasing available memory : #Spark Submit and options that can be used along with Spark Submit

#Overview of Technologies Involved:
1.HDFS 	            Unix like commands                  : The Hadoop Distributed File System(HDFS) offers a way to store large files across multiple machines.
2.Sqoop 	        Unix like commands with some SQL    : Framework for bulk data transfer between HDFS and structured datastores as RDBMS.
3.Spark 	        Scala OR  Python                    : It is a Data analytics cluster computing framework. Spark fits into the Hadoop open-source community, building on top of the Hadoop Distributed File System(HDFS). To its credit, Spark provides an easier to use alternative to Hadoop MapReduce and offers performance up to 10 times faster than previous generation systems like Hadoop MapReduce for certain applications.Spark is a framework for writing fast, distributed programs. Spark solves similar problems as Hadoop MapReduce does but with a fast in-memory approach and a clean functional style API. For the certification exam, the emphasis is on Spark and not on tradition map reduce.
4.Spark RDD 	    Scala OR  Python                    : RDD stands for Resilient Distributed Datasets(RDD) is a fundamental data structure of Spark. It is an immutable distributed collection of objects. Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.
5.Spark DF 	        Scala OR  Python                    : A DataFrame is a distributed collection of data, which is organized into named columns.  A DataFrame can be constructed from an array of different sources such as Hive tables, Structured Data files, external databases, or existing RDDs.
6.Spark Streaming 	Scala OR  Python                    : Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Spark Streaming provides a high-level abstraction called discretized stream or DStream, which represents a continuous stream of data
7.Spark SQL 	    Scala, Python and SQL               : Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations.
8.Spark Submit 	    Unix like commands                  : it is a mechanism to run spark programs as applications by suppliying configurable parameters to optimize spark code execution
9.Flume 	        Unix like commands 	                : Flume is a distributed, reliable, and available service for efficiently collecting, aggregating, and moving large amounts of log data. It has a simple and flexible architecture based on streaming data flows. It is robust and fault tolerant with tunable reliability mechanisms and many failover and recovery mechanisms. It uses a simple extensible data model that allows for online analytic application.
10.Hive 	        SQL                                 : Hive provides a SQL-like interface to data stored in HDP.
11.Spark Streaming 	Scala and Python                    : Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Spark Streaming provides a high-level abstraction called discretized stream or DStream, which represents a continuous stream of data
12.Impala 	        SQL                                 : Cloudera's very own hive like interface but uses its own engines instead of relying on spark or map reduce engine for processing.


#Getting Started - HDFS Quick Preview
@HDFS
Properties files:
/etc/hadoop/conf/core-site.xml
/etc/hadoop/conf/hdfs-site.xml

@Important Properties
fs.defaultFS
dfs.blocksize
dfs.replication

@HDFS Commands
#Copying Files:
From local file system (hadoop fs -copyFromLocal or -put)
To local file system (hadoop fs -copyToLocal ot -get)
From one HDFS location to other (hadoop fs -cp)
#Listing files (hadoop fs -ls)
#Previewing data from files (hadoop fs -tail or -cat)
#Checking sizes of the files (hadoop fs -du)

Ip address
Name Node UI will be running on Port No: 50070
IP address of Name Node UI will be given. 
Else most likely :50070 will be name node UI. 
Primarily Live Nodes information should be checked.

Even if they don't give Name Node details, you should be able to get the information.
No matter what evn you are using, you should be able to hop on to 
gateway node or client node of your cluster.

#Works on CDH cluster 
cd /etc/hadoop/conf
ls -ltr 
view core-site.xml 

#core-site.xml and hdfs-site.xml
core-site.xml : gives important information about your cluster. 
fs.defaultFS : gives URI for your nade node. This is where the master daemon process of your HDFS will be running.
Using the URL, you can use convert and get the Web UI url.

#Default name node URI:
fs.defaultFS : hdfs://quickstart.cloudera:8020
#To convert it to web UI : 
http://quickstart.cloudera:50070 

#hdfs-site.xml 
Important properties are:
#dfs.blocksize : will tell what should be the block size your file should be divided. 
If 10GB needs to be divided, this will give the size of divided block sizes. Distributed capacility for your file system.
#dfs.replication : failover and fault tolerance. 
By default 3. So, data will be having 3 copies. 

hostname -f : gives hostname on command line
cp : copy files
mkdir : make sirectory

#Hadoop Commands 
hadoop fs : command line interface to do actions. 
hadoop fs -ls /user/srikapardhi // you need access of super user you should be able to create. 
sudo -u hdfs hadoop fs -mkdir /user/root; sudo -u hdfs fs -chown -R root /user/root #for hortonworks
On cloudera quickstart : cloudera user has already have user space for you by default. 

#Help commands 
hadoop fs -help copyFromLocal 
hadoop fs -help put 

#Commands: 
copyFromLocal location Destination

#always better to end /. to copy everything including crime folder too. 
#if srikapardhi doesn't exist then crime will be copied including srikapardhi if /. is not present. 
#Copies all contents of crime to srikapardhi
hadoop fs -copyFromLocal /data/crime /user/srikapardhi/.

#Shows all data including subfolders on crime folder. 
#copyFromLocal is almost identical to -put command. 
hadoop fs -ls -R /user/srikapardhi/crime

#Gives you the size of the crime folder 
hadoop fs -du -s -h /user/srikapardhi/crime 
#-h stands for readable representation of the size.

hdfs fsck /user/srikapardhi/crime -files -blocks -locations 
#Gives files blocks and location information. 

Notes : As replication is 3, each of the file block is stored on 3 nodes with ip address of that noode. 
#Notes
hadoop fs -copyFromLocal /data/retail_db /user/srikapardhi 

#Shows the number of records in the data 
wc -l /data/retail_db/*/* 

#Getting Started - YARN Quick Preview
@YARN 
YARN stands for Yet Another Resource Negotiator. 

In certifications Spark typically runs in YARN mode. 
We should be able to check the memory configuration to understand the cluster capacity. 

@Important Properties
/etc/hadoop/config/yarn-site.xml
/etc/spark/conf/spark-env.sh 

@Spark default settings 
Number of executors 2
Memory 1GB 

We under utilize resources. Understanding settings throughly and then mapping them with data size we are 
trying to process, we can accelerate the execution of our jobs.

Map Reduce, Tez, any framework can leverage YARN and execute the jobs in appropiate fasion. So that jobs can be queued, failer over etc.  
Like HDFS, YARN also has a web UI. We can pick from conf file.

cd /etc/hadoop/conf 
vi yarn-site.xml

#Resource Manager
yarn.resourcemanager.address 
yarn.recourcemanager.webapp.address

#Not available in current cluster. 

cd /etc/spark/conf 
vi spark-env.sh

#Default properties 
Executor cores = 1 for each job if it is less than vcores total from recource manager total. 
Most probably use less than 50% of the resource. 
Get data set size from hadoop fs -du , 
Executor memory = 1GB 
Each task can take 1 core. By mapping vcores total in nodes of the cluster in hadoop yarn resource manager. 

#Resource Manager - VCores total and Memory total.
Understanding memory resources throughly and mapping them to the actual data size can fasten processes. 

#SetUp Data Sets 
@Tables
departments
categories
products
order_items
orders
customers

#Apache Sqoop - Introduction and objectives
@Apache Sqoop
Max 2-3 Questions from Sqoop.

#Apache Sqoop - Accessing Documentation
Documentation : http://archive-primary.cloudera.com/cdh5/cdh/5/sqoop-1.4.6-cdh5.15.0/SqoopUserGuide.html
Sqoop User Guide Version: 1.4.6 

#5 important Sqoop commands for certification
sqoop import
sqoope export 
sqoop eval
sqoop list-databases
sqoop help

#example: sqoop help eval

#Apache Sqoop - Validating MySQL and Environment
@Validating MySQL and Environment 

Mac and Linux : Use terminal directly
Windows : Cygwin 
on Cloudera CDH mysql username : root password : cloudera 

#To login : For certification you don't need to connect to database using MySQL 
>mysql -u retail_user -h hostname -p 

mysql> show databases; 
mysql> use retail_db;
mysql> show tables;
mysql> select * from orders limit 10; 

#Apache Sqoop - listing databases and tables
Connect String
Giving the password
Listing databases
Listing tables

Documentation help : 
Make sure the jar file for jdbc is placed in the location. 
For certification, jar file will be placed at the right location.
For project purpose, check where are the sqoop binaries. 
Default Port No on which mysql is running : 3306 
Password : Hardcoding, prompting or password file. 

#Examples 
>sqoop help
>sqoop help list-databases
>sqoop-list-databases #Both are syntaxtically correct 

#List Databases 
sqoop list-databases --connect jdbc:mysql://ms.itversity.com:3306 --username retail_user --password itversity 

Typically sqoop commands, we will enclose in shell commands or any other scripting language. 
--p #prompt 
--password #hardcoded password - sufficient for certification 
--password-file #fully qualified path location

#List Tables
sqoop list-tables --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user --password itversity 

@Sqoop Quering from databases using eval 
#Apache Sqoop - Querying from Databases using eval
Running queries - we can use eval to run queries on remote datases.

>sqoop help eval
#We can even use -e or --query 
#eval is used in automation also before and after import.

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query "SELECT * FROM orders LIMIT 10"

#No Access to table
sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query "INSERT INTO orders VALUES (100000, '2017-10-31 00:00:00:0', 100000, 'DUMMY')"

#DDL 
sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query "CREATE TABLE dummy (i INT)"

#For testing automation, eval is widely used - for validations and automations.  

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query "INSERT INTO dummy VALUES (1)"

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query "SELECT * FROM dummy"

@Sqoop Import Running Simple Import 
#Apache Sqoop - Sqoop Import - Run simple import
Simple import
Default behaviour

#Code
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
#--target-dir /user/srikapardhi/sqoop_import/retail_db/order_items
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db

#target directory : HDFS destination dir  
#warehouse dirctory : HDFS for table destination

in target-dir what ever destination is given that should not exist earlier. 
With warehouse directory whatever dir we are passing can exist and under that it will create another directory with table name. 

#Validate 
hadoop fs - ls /user/srikapardhi/sqoop_import/retail_db
#Can check data using tail command.
hadoop fs - tail /user/srikapardhi/sqoop_import/retail_db/order_items/part-m-00000 

#Default behaviour of sqoop import is , what ever data is copied, it will be , 'comma' delimiter. 

#Apache Sqoop - Sqoop Import - Execution Life Cycle
Simple import
Execution life cycle 
Number of mappers

Default behaviour : if directory already exists, it will fail complaining di already exists.
#Remove file:
hadoop fs -rm -R /user/srikapardhi/sqoop_import/retail_db/order_items/
#Execute the previous command again. 

Life Cycle :
1. Generate Map Reduce code/ Java
2. Compile jar file and submit that jar file for Import
3. Runs select query to execute a dummy query, to fetch the metadata / structure of the data to prepare an object in java. 
   Each unique table, will have a unique java class that will be exact structure of the data metadata.
4. By default sqoop uses 4 threads --num-mappers, -m . by default the num mappers is 4. 
5. So, total 4 threads, each thread will copy mutually exclusive subsets. 
6. Default logic for division of data is : MIN, MAX of primary key. 
   If No. of mappers are 4, then 172198 will be divided among 4. 
7. in logs URL to track the job: Go to the url and you can see the counters information. Click on Map Tasks and get task level information.

Note : If you are copying 1GB of data, mostly you can use 8 mappers.There are concerns, check with source also for resources.

#Code
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 1
#Now launched map tasks is only 1. It don't execute the boundary conditions now as there is only 1 thread and don't divide into no of subsets. 
#Understand side effects of increasing no of threads. It can cause overhead on source db. 

@Sqoop Import Managing Directories 
#Apache Sqoop - Sqoop Import - Managing Directories
1. Target Directory 
2. Warehouse Directory 
3. Managing Directories 
>Overwriting existing directory 
>Append to existing directory 

Target Directory : Is nothing but target HDFS directory which means files will be created under this directory.
Warehouse Directory: Whatever directory you have given, a subdirectory will be created with the table name.
if the query is run again, it will complain that dir already exists.


#Delete Target Directory 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 1
--delete-target-dir

#Append to existing directory 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 1
--append 

#Typically Append is used to get the delta. There will be two files. 
Append and Delete target dir are mutually exclusive. They should not be used together. 
Append or over write the existing directory. Only these two facilities. 
Warehouse directory : it will actually create the sub directories under warehouse dir with the names of the tables and copy the data to them. Useful to facilitate faster automation. 
To copy multiple tables to HDFS

@Import using splot by 
#Apache Sqoop - Sqoop Import - using split by
1. Number of mappers
2. Splitting using custom column
3. Splitting using non numeric column 

Mappers are used to divide the reading the data. No. of mappers also decide the no of files to be created. 
If column on which data need to be partitioned contains 'NULL Values' which partition those NULL Values should go
Because of this it will be using primary key. Because primary key will be indexed, no null values will be present. 
Unique, not null, indexed - so sqoop will use that to index to divide min and max.

What if table doesn't have 'PRIMARY KEY'? it will fail. It cannot import the data, especially with no. of mappers 4.
If the number of mappers is 1 then it will work. 

#Create table with no primary key.
create table order_items_nopk as select * from order_items; 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items_nopk \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db
--split-by order_item_id 

#Order_item_id , order_item_product_id - values will be unique so will use them. 

#//Things to remember for split-by 
--Split-by (column_name) 
1. Column should be indexed. It will scan the entire table and it will take time. 
2. Select * from order_items_nopk where order_item_id >= 1 and order_item_id < 43049. Reading indexed columns will be much faster. 
3. Also often it should be sequence generated or evenly increented
4. It should not have null values. 
5. If there are null by values in split-by they will be ignored. 
6. Values in the field should be sparse. 

#Columns should be indexed for performance reasons. 
We need to specify a column which is indexed. 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db
--split-by order_status  

#Will fail as order_status is a non numeric field. Splitting on string field is not straight forward. 

sqoop import \
--Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db
--split-by order_status  

#You can update the sqoop.properties, --Dorg.apache.sqoop.splitter.allow_text_splitter=true so that you don't need to pass the property always. 
No of splits as 5. Side effects of using non numeric field as split by column 

#verification 
hadoop fs - ls /user/srikapardhi/sqoop_import/retail_db/orders
hadoop fs -tail [file name] 

@Auto reset to one mapper based on table
#Apache Sqoop - Sqoop Import - auto reset to one mapper 
Automatic mapper number logic - Auto Reset the number of mappers to 1 if 
there is no primary key on certain tables if we are importing multiple tables.

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items_nopk \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db
--autoreset-to-one-mapper 

#Now it will use only 1 thread whenever there is no primary key on a table. The app auto decides whenever there is a reuirement. 

@File Formats Import 
#Apache Sqoop - Sqoop Import - File formats
Text file (default)
Sequence file
Avro file
Parquet file

Sequence file : Binary data 
Avro file  : Binary json format
Parquet file : Columnar file format

#sqoop help import 

--as-textfile 
--as-sequencefile
--as-avrodatafile
--as-parquetfile

MPP : Masively Parallel Processing 

#Delete all the old data recursively to make it clean 
hadoop fs -rm -R /user/srikapardhi/sqoop_import/retail_db/*

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 2 \
--as-sequencefile 
#--as-avrodatafile
#--as-parquetfile

hadopp fs -ls /user/srikapardhi/sqoop_import/retail_db/order_items 

hadoop fs -tail /user/srikapardhi/sqoop_import/retail_db/order_items/part-m-00000

#tail will show only 1KB of data. Don't do this in exam, it will hang the terminal.

@Compression Algoorithms Sqoop Import 
#Apache Sqoop - Sqoop Import - Compressing the data
Gzip
Deflate
Snappy
Others

#Hadoop required 3x of the storage for replication purposes. Each of the file will be stored with 3 copies which can be customized. 

--compress, -z [arguments for compression] enable compression 
--compression-codec [default is gzip]

#Delete the previous data inserted
hadoop fs -rm -R /user/srikapardhi/sqoop_import/retail_db/* 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 2 \
--as-textfile \  #Optional because it is default 
--compress  

#As files are compressed, we can't do cat or tail on the file. 
hadoop fs -get /user/srikapardhi/sqoop_import/retail_db/order_items order_items 
cd order_items 

#To unzip the files 
gunzip part*.gz 
ls -ltr 
view or tail can be used to view them. 
rm -rf order_items 
#dropping the dir and cleaning the hdfs path. 

#For Codecs available and fully classified class path information 
cd /etc/hadoop/conf
view core-site.xml 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 2 \
--as-textfile \  #Optional because it is default 
--compress \ #first enable compression 
--compression-codec org.apache.hadoop.io.compress.SnappyCodec #Pass compression codec classname with fully qualified classname 

>compression.codecs : snappycodec, Gzipcodec
Sqoop default : gzip
Hadoop default: DefaultCodec 

@Boundary Query
#Apache Sqoop - Sqoop Import - Boundary query
Boundary Query
Columns 
Query 

#Delete files which are already created 

#Simple import 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--boundary-query 'select min(order_item_id), max(order_item_id) from order_items where order_item_id > 99999'

#Hardcode values : to get data whichin a range to avoid resource wastes. 
sqoop import \
--connect jdbc: mysql://ms.itversity.com: 3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--boundary-query 'select 100000, 172198' 

#Boundary query should always give min and max values. 
boundary query in combination of split by if trying to get boundary from non primary key column. 

@Transformations and filtering in sqoop
#Apache Sqoop - Sqoop Import - Columns and query
1.Boundary Query 
2.Transformations and filtering 
a.Columns 
b.Query 

#Notes:
If you want to get subsets of columns (say from 10 columns, you just need 4) 
you can use columns and if you have conditions you can use query. 
Query and Table are mutually exclusive.
Query cannot be used along with columns, either you use table and or columns or use query. (query is more generic and flexible) 
Table and or columns are mutually exclusive with query. 
Query should not be used with table or table and colums. 
If you are using table, you should have colums also.  
if you are using colums, you should use table also. 

#a.Columns 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--columns order_item_order_id,order_item_id,order_item_subtotal \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--num-mappers 2

#columns is closely related to table. 
--columns should be with comma separated, and there shouldn't be space after column names. 
#verification
hadoop fs -tail /user/srikapardhi/sqoop_import/retail_db/order_items/part-m-00000
#Subset of columns that you are interested on table.

#b.Query : When you use query you should neither use table nor columns. 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/srikapardhi/sqoop_import/retail_db/orders_with_revenue \
--num-mappers 2
--query "select o.*, sum(oi.order_item_subtotal) order revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id and \$CONDITIONS group by o.order_id, o.order_date, o.order_customer_id, o.order_status" \
--split-by order_id 

#When using query, warehouse cannot be used, we need to use target-dir and split-by needs to given as the tool doesn't know how to divide the data between mappers if there are more than 2 mappers. 
$CONDITIONS - is a placeholder for the convenience of sqoop developers. 

#Verification 
hadoop fs -tail /user/srikapardhi/sqoop_import/retail_db/orders_with_revenue/part-m-00000

#Table and/or columns is mutually exclusive with query 
#For query split by is mandatory if num mappers is greater than 1 - coz it doesn't know how to split 
#query should have a placeholder for \$conditions

@Sqoop Import Delimiters and Handling nulls 
#Apache Sqoop - Sqoop Import - Delimiters and handling nulls
Delimiters 
Handling nulls
#Different db called HR is used to demonstrate this example.

>mysql -u hr_user -h ms.itversity.com -p 
>itversity
mysql> show databases;
mysql> use hr_db;
mysql> show tables; 
mysql> exit 
Bye 

#Code 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/hr_db \
--username hr_user \
--password itversity \
--table employees \
--warehouse-dir /user/srikapardhi/sqoop_import/hr_db

commission_pct field in table has NULL values.

Select commission_pct from employees; 
in traditional db NULL represents nothing.
The way NULL will be treated to RDBMS is properitory to it's db.
But in HDFS the treatment should be different for NULL. 

hadoop fs - get / user/srikapardhi/sqoop_import/hr_db

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/hr_db \
--username hr_user \
--password itversity \
--table employees \
--warehouse-dir /user/srikapardhi/sqoop_import/hr_db
--null-non-string -1 \
--fields-terminated-by "\t" \
--lines-terminated-by  ":"

#Output line formatting arguments 
--optionally-enclosed-by 
--fields-terminated-by
--null-non-string

#Numeric Data types : Null string is represented by -1 
\000 - ASCII NULL 
#Most likely they will give the delimiter information. 

@Sqoop Import Incremental Import 
#Apache Sqoop - Sqoop Import - Incremental loads
Incremental import
1. Using Query
2. Using Where
3. Alternative 

Capture the delta and load the latest data into database. 
Inserts are easy to cpature. 
Updates/Delete - it is a bit complicated, we need to work with source team. 
Delete is a bit more complicated. There is no easy way to capture the information for use in downstream app like HDFS. Additional flag for delete called soft delete. Set a flag to true if the record is deleted. 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/srikapardhi/sqoop_import/retail_db/orders \
--num-mappers 2
--query "select * from orders where \$CONDITIONS and order_date like '2013-%'" \
--split-by order_id 


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/srikapardhi/sqoop_import/retail_db/orders \
--num-mappers 2
--query "select * from orders where \$CONDITIONS and order_date like '2014-01%'" \
--split-by order_id \
--append #Append flag, data will be copied to the existing directory, otherwise it will fail. 

#Drawback of query : We need to specify split-by. Identifying field is difficult if there are many tables. 
#Sqoop User Guide 

#Advantage of where clause is we can still use the table.  
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/srikapardhi/sqoop_import/retail_db/orders \
--num-mappers 2
--table orders \
--where "order_date like '2014-02%'" \
--append 

If we use where condition or query, we don't know upto what data we already copied or updated everytime. 
We need to run eval before and after the import to check the data values.But these things are taken care if we use official incremental import arguments. 

#Official Incremental Import using sqoop
--check-colum 
--incremental
--last-value 

The above will actually display the value 
#Sqoop supports two types of incremental imports : append(typically with append mode we use numeric fields) and last modified (primary key fields on insert only tables)
last modifed - where updates are done on source table, typically the column which will be passing as check is date field. 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/srikapardhi/sqoop_import/retail_db/orders \
--num-mappers 2
--table orders \
--check-column order_date \
--incremental append \
--last-value '2014-02-28' 

#Capture the importool information and save it somewhere, so that next time we can continue from the left over. 
#'Sqoop job --create' will take over this job and take care of the last import details too. 

@Sqoop Import to Hive tables
#Apache Sqoop - Sqoop Import - Create database in hive
1. Simple Hive Import
2. Managing tables while performing Hive import 

>hive 
>create database srikapardhi_sqoop_import; 
if database already exists it fails.
>use srikapardhi_sqoop_import
#Validate hive is working for you. Make sure you switch to database created.
>create table t (i int);
>insert into table t values (1);
>select * from t;
> drop table t;

@Sqoop Simple Hive Import 
#Apache Sqoop - Sqoop Import - Simple hive import
#Sqoop User Guide > Importing Data into Hive. 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--hive-import \
--hive-database srikapardhi_sqoop_import \
--hive-table order_items \
--num-mappers 2 

#--hive-import (Uses hive's default delimiters if none are set) 

Mysql > Sqoop
Sqoop > temporary DIR 
Temporary DIR > Hive tables 
Hive table is nothing but a HDFS directory. 

>hive 
>use srikapardhi_sqoop_import;
>show tables;
>describe formatted order_items; 
#Will contain Location of the Hive table.

>hadoop fs -ls 'path' 
>hadoop fs -get 'path' srikapardhi_sqoop_import_order_items 
>cd srikapardhi_sqoop_import_order_items
>view part-m-00000
#Default Delimiter in Hive for fields is ^A or ctrl A or ASCII 1 
\u0001 - defailt field delimiter 

@Sqoop Hive Import and Managing Tables 
#Apache Sqoop - Sqoop Import - Hive import - Managing tables

sqoop import \
--connect jdbc: mysql://ms.itversity.com: 3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--hive-import \
--hive-database srikapardhi_sqoop_import \
--hive-table order_items \
--num-mappers 2

>hive 
>use srikapardhi_sqoop_import;
>show tables;
>describe formatted order_items;
#In hive the table is nothing but a HDFS location. 
> select count(1) from order_items;
>exit;

#Overwrite instead of appending by default. 
sqoop import \
--connect jdbc: mysql://ms.itversity.com: 3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--hive-import \
--hive-database srikapardhi_sqoop_import \
--hive-table order_items \
--hive-overwrite \
--num-mappers 2

#This will drop the table, recreate it. 

#3rd Method : If the target table exists, then the command will fail using this syntax.
--create-hive-table 
This is highly confusing syntax. By default this property is false. 
#Code 
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--hive-import \
--hive-database srikapardhi_sqoop_import \
--hive-table order_items \
--create-hive-table \
--num-mappers 2

# Use - hive overwrite or create hive table. Don't use both.  
#Most Buggy Syntax on Sqoop import 

the import will fail and data copied to Staging location before copying to target location. If sqoop import will fail, the data saved to staging loc will be left without clean up. 

#Check - hadoop fs -ls /user/srikapardhi 
#Error comes for staging location. 

hadoop fs -rm -R /user/srikapardhi/order_items 

1. --append
2. --Overwrite
3. --create-hive-table = fail if target already exists 

#Even in projects we don't consider partition value directly. 

--map-column-hive : typically data type on source will be different from hive. Typically the intelligence will map the data types but at times we might want to customize. SO, instead of using defaults, we can use custom data types for columns in hive while importing when required.

@Sqoop Import All Tables
#Apache Sqoop - Sqoop Import - Import all tables
1.Import all tables
2.Limitations
a. --warehouse-dir is mandatory
b. Better to use auto-reset-to-one-mapper 
c. Cannot specify many arguments such as --query, --cols, --where, which does filtering on transformations on the data
d.Incremental import not possible


sqoop import-all-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--warehouse-dir /user/srikapardhi/sqoop_import/retail_db \
--autoreset-to-one-mapper
#Avoid dir with table names. 

@Typical Life Cycle of Sqoop Data processing 
#Apache Sqoop - Typical life cycle
1. Data Ingestion using sqoop - one of the approaches
2. Process Data
3. Visualize the processed data 
a. Connect BI/Visualization tools to HDFS directly 
b. Port the proessed data to a database 
4. Sqoop export will help you in porting the process data to a database. 

Import Orders and order_items tables using sqoop. 
describe orders;
describe order_items;
#Relation : order_item_id = order_id 
#Orders and order_items are transaction tables. 

#HIVE Query
create table daily_revenue as 
select order_date, sum(order_item_subtotal) daily_revenue
from orders join order_items on
order_id = order_item_order_id
where order_date like '2013-07%'
group by order_date; 

#Validation
describe daily_revenue;
select * from daily_revenue;


@Sqoop Export with delimiters 
#Apache Sqoop - Sqoop Export - Simple export with delimiters
1. Simple export
2. Delimiters
3. Export behaviour
4. Number of mappers 


















#Spark Learning :
The Spark engine – Spark core components and their implications 
1.Driver program
2.Spark shell 
3.SparkContext 
4.Worker nodes 
5.Executors 
6.Shared variables
7.Flow of execution

Explanation :
1.Driver program
2.Spark shell 
3.SparkContext 
4.Worker nodes 
5.Executors : Each application has a set of executor processes. Executors reside on worker nodes and communicate directly with the driver once the connection is made by the cluster manager. 
All executors are managed by SparkContext. 
6.Shared variables : There are two kinds of shared variables, broadcast variables and accumulators.
7.Flow of execution

The RDD API – understanding the RDD fundamentals 
1.RDD basics
2.Persistence

RDD operations – let's get your hands dirty 
1.Getting started with the shell
2.Creating RDDs
3.Transformations on normal RDDs 
4.Transformations on pair RDDs 
5.Actions

Abbreviations:
Resilient Distributed Dataset (RDD)
Directed Acyclic Graph (DAG)

Two ways to create spark RDDs : 
1. parallelize existing collection
2. reference an external storage such as filesystem

Two methods available on RDD :
1. Transformations : Methods to create RDDs
2. Actions : Methods that utilize RDD 

RDDs are immutable, so any Transformations leads to new creation of RDD. 
Transformations are applied lazily, only when an action is applied to them and not when an RDD is defined. 
An RDD is recomputed everytime it is used in an action, unless the user explicitely persists the RDD in memory. 
Lazy Transformations Advantage: Optimize the transformation steps. 

Narrow Dependency : 
Wide Dependency : 

Persistence : 
RDDs are computed everytime on the fly everytime they are acted upon the action method.
A developer has options to save persist or cache a dataset in memory across locations.
Developer may remove unwanted RDDs using unpersist.

Nevertheless, LRU algorithms - least recently used , monitors the cache and removes old partitions. 

cache() - Designed Only for persristance in the memory
persist() = different levels of persistence , memory, only memory, disk and only disk.
persist(MEMORY_ONLY)


Two ways to create an RDD.
1. Creating RDDs from the file system source. 
fileRDD = sc.textFile('READMED')
type(fileRDD)
fileRDD.first()

2. Parallelize an existing collection.
numRDD = sc.parallelize([1,2,3,4],2) // This two defines the number of partitions the data should be divided explicitely. 
Though this partitioning is done automatically. 1 2 , 3 4 
type(numRDD)
numRDD.first()

numRDD.map(lambda x : x * x).collect()
numRDD.map(lambda x : x * x).reduce(lambda a,b : a+b)

lambda Function is an unnamed function typically used as function arguments to other functions. 

Transformations on normal RDDs:
1. filter operation
2. distinct operation
3. intersection operation
4. union operation
5. map operation
6. flatmap operation
7. keys operation
8. cartesian operation

1. filter operation : 
The filter operation returns an RDD with only those elements that satisfy a filter condition, similar to the WHERE condition in SQL.
a = sc.parallelize([1,2,3,4,5,6,7,8,9])
b = a.filter(lambda x: x % 3 == 0)
b.collect()


2. distinct operation :
The distinct ([numTasks]) operation returns an RDD with a new dataset after eliminating duplicates. 
Returns distinct element in the RDD. 
c = sc.parallelize(["John", "Jack", "Mike", "Jack"])
c.distinct().collect()


3. intersection operation : 
The intersection operation takes another dataset as input. It returns a dataset that contains common elements:
x = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
y = sc.parallelize([5,6,7,8,9,10,11,12,13,14,15])
z = x.intersection(y)
z.collect() 
#Or can use take 
or i in z.take(8): print(i)


4. union operation : 
The union operation takes another dataset as input. It returns a dataset that contains elements of itself and 
the input dataset supplied to it. If there are common values in both sets, then they will appear as duplicate values 
in the resulting set after union:
a = sc.parallelize([3,4,5,6,7], 1)
b = sc.parallelize([7,8,9], 1)
c = a.union(b)
c.collect()


5. map operation
The map operation returns a distributed dataset formed by executing an input function on each of the elements in the input dataset:
 map(func, *iterables) --> map object
 |
 |  Make an iterator that computes the function using arguments from
 |  each of the iterables.  Stops when the shortest iterable is exhausted.

a = sc.parallelize(["animal", "human", "bird", "rat"], 3)
b = a.map(lambda x: len(x))
#[6, 5, 4, 3]
c = a.zip(b) --help on class zip(object) builtin module 
c.collect() 
#[('animal', 6), ('human', 5), ('bird', 4), ('rat', 3)]


6. flatmap operation
The flatMap operation is similar to the map operation. While map returns one element per input element, 
flatMap returns a list of zero or more elements for each input element:
sc.parallelize([1,2,3,4,5], 4).flatMap(lambda x: range(1,x+1)).collect()
[1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5]
sc.parallelize([5, 10, 20], 2).flatMap(lambda x:[x, x, x]).collect()
[5, 5, 5, 10, 10, 10, 20, 20, 20]


7. keys operation
The keys operation returns an RDD with the key of each tuple
a = sc.parallelize(["black", "blue", "white", "green", "grey"], 2)
b = a.map(lambda x:(len(x), x))
[(5, 'black'), (4, 'blue'), (5, 'white'), (5, 'green'), (4, 'grey')]
c = b.keys()
c.collect()
[5, 4, 5, 5, 4]


8. cartesian operation
The cartesian operation takes another dataset as argument and returns the Cartesian product of both datasets. 
This can be an expensive operation, returning a dataset of size m x n where m and n are the sizes of input datasets:

x = sc.parallelize([1,2,3])
y = sc.parallelize([10,11,12])
x.cartesian(y).collect()
[(1, 10), (1, 11), (1, 12), (2, 10), (2, 11), (2, 12), (3, 10), (3, 11), (3, 12)]



Transformations on pair RDD: 
Some Spark operations are available only on RDDs of key value pairs. Note that most of these operations, 
except counting operations, usually involve shuffling, because the data related to a key may not always reside on a single partition.
1. groupbyKey operation
2. join operation
3. reduceByKey operation
4. aggregate operation



1. groupbyKey operation
Similar to the SQL groupBy operation, this groups input data based on the key and you can use 
aggregateKey or reduceByKey to perform aggregate operations:
a = sc.parallelize(["black", "blue", "white", "green", "grey"], 2)
b = a.groupBy(lambda x: len(x)).collect() 
[(4, <pyspark.resultiterable.ResultIterable object at 0x10680bdd8>), (5, <pyspark.resultiterable.ResultIterable object at 0x10680be80>)]
sorted([(x,sorted(y)) for (x,y) in b])
[(4, ['blue', 'grey']), (5, ['black', 'green', 'white'])]

2. join operation
The join operation takes another dataset as input. Both datasets should be of the key value pairs type. 
The resulting dataset is yet another key value dataset having keys and values from both datasets.
a = sc.parallelize(["blue", "green", "orange"], 3)
b = a.keyBy(lambda x: len(x))
c = sc.parallelize(["black", "white", "grey"], 3)
d = c.keyBy(lambda x: len(x))


b.join(d).collect()
[(4, ('blue', 'grey')), (5, ('green', 'black')), (5, ('green', 'white'))]

//leftOuterJoin
b.leftOuterJoin(d).collect()
[(6, ('orange', None)), (4, ('blue', 'grey')), (5, ('green', 'black')), (5,('green', 'white'))]

//rightOuterJoin
b.rightOuterJoin(d).collect()
[(4, ('blue', 'grey')), (5, ('green', 'black')), (5, ('green', 'white'))]

//fullOuterJoin
b.fullOuterJoin(d).collect()
[(6, ('orange', None)), (4, ('blue', 'grey')), (5, ('green', 'black')), (5,('green', 'white'))]



3. reduceByKey operation
The reduceByKey operation merges the values for each key using an associative reduce function. 
This will also perform the merging locally on each mapper before sending results to a reducer and producing hash-partitioned output.
a = sc.parallelize(["black", "blue", "white", "green", "grey"], 2)
b = a.map(lambda x: (len(x), x))
b.collect()
[(5, 'black'), (4, 'blue'), (5, 'white'), (5, 'green'), (4, 'grey')]
b.reduceByKey(lambda x,y: x + y).collect()
[(4, 'bluegrey'), (5, 'blackwhitegreen')]

#Will add the values/Strings of same KeyValues. Example : All 4, 5


4. aggregate operation #Check this operation 
The aggregrate operation returns an RDD with the keys of each tuple.

z = sc.parallelize([1,2,7,4,30,6], 2)
z.aggregate(0,(lambda x, y: max(x, y)),(lambda x, y: x + y))
37

z = sc.parallelize(["a","b","c","d"],2)
z.aggregate("",(lambda x, y: x + y),(lambda x, y: x + y))
'abcd'

z.aggregate("s",(lambda x, y: x + y),(lambda x, y: x + y))
'ssabsscds'

z = sc.parallelize(["12","234","345","56789"], 2)
z.aggregate("",(lambda x, y: str(max(len(str(x)), len(str(y))))),(lambda x,y: str(y) + str(x)))
'53'

z.aggregate("",(lambda x, y: str(min(len(str(x)), len(str(y))))),(lambda x,y: str(y) + str(x)))
'11'

z = sc.parallelize(["12","234","345",""],2)
z.aggregate("",(lambda x, y: str(min(len(str(x)), len(str(y))))),(lambda x,y: str(y) + str(x)))
'01'


Actions: Once an RDD is created various transformations get executed only when an action is performed on it. 
The result of an action can either be data written back to the storage system or returned to the driver program 
that initiated this for further computation locally to produce the final result.
1. collect()
2. count()
3. take(n)
4. first()
5. takeSample()
6. countByKey()


Calling collect () on an RDD will return the entire dataset to the driver which can cause out of memory and we should avoid that.
Collect (Action) - Return all the elements of the dataset as an array at the driver program. 
This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.


1. collect()
The collect() function returns all the results of an RDD operation as an array to the driver program. 
This is usually useful for operations that produce sufficiently small datasets. 
Ideally, the result should easily fit in the memory of the system that's hosting the driver program.

2. count()
This returns the number of elements in a dataset or the resulting output of an RDD operation.

3. take(n)
The take(n) function returns the first (n) elements of a dataset or the resulting output of an RDD operation.

4. first()
The first() function returns the first element of the dataset or the resulting output of an RDD operation. 
It works similarly to the take(1) function.

5. takeSample()
The takeSample(withReplacement, num, [seed]) function returns an array with a random sample of elements from a dataset. 
It has three arguments as follows:

1. withReplacement/withoutReplacement: This indicates sampling with or without replacement (while taking multiple samples, 
it indicates whether to replace the old sample back to the set and then take a fresh sample or sample without replacing). 
For withReplacement, argument should be True and False otherwise.
2. num: This indicates the number of elements in the sample. 
3. Seed: This is a random number generator seed (optional).

6. countByKey()
The countByKey() function is available only on RDDs of type key value. It returns a table of (K, Int) pairs with the count of each key.


#DataFrames - Introduction
Interactive data analysis gets easier with datasets that can be represented as named columns, 
which was not the case with plain RDDs. So, the need for a schema-based approach to represent data 
in a standardized way was the inspiration behind DataFrames.


1.Why DataFrames?
2.Spark SQL
>Catalyst optimizer
3.DataFrame API
>DataFrame basics
>RDD versus DataFrame
4.Creating DataFrames 
>From RDDs
>From JSON
>From JDBC sources 
>From other data sources
5.Manipulating DataFrames


1.Why DataFrames?
Though Spark provided a functional programming API to manipulate distributed collections of data, it ended up with tuples (_1, _2, ...)
Coding to operate on tuples was a little complicated and messy, and was slow at times. 
So, a standardized layer was needed, with the following characteristics:
1.Named columns with a schema
2.Functionality to consolidate data from various data sources such as 
Hive, Parquet, SQL Server, PostgreSQL, JSON, and also Spark's native RDDs, and unify them to a common format
3.Ability to take advantage of built-in schemas in special file formats such as Avro, CSV, JSON, and so on.


2.Spark SQL
Spark SQL can also be used to read data from an existing Hive installation. 
Apart from these plain SQL operations, Spark SQL also addresses some tough problems. 
Designing complex logic through relational queries was cumbersome and almost impossible at times. 
So, Spark SQL was designed to integrate the capabilities of relational processing and 
functional programming so that complex logics can be implemented, optimized, and scaled on a distributed computing setup.

There are basically three ways to interact with Spark SQL, including SQL, the DataFrame API, and the Dataset API.

Through the declarative syntax, users can focus on what the program should accomplish and 
not bother about the control flow, which will be taken care of by the Catalyst optimizer, built inside Spark SQL.

>Catalyst optimizer
1.Spark has built-in support for JSON schema inference. 
Users can just create a table out of any JSON file by registering it as a table and simply query it with SQL syntaxes.
2.built-in support for CSV, XML, and other formats



3.DataFrame API
A two-dimensional data structure that usually has labelled rows and columns is called a DataFrame in some realms, 
such as R DataFrames and Python's Pandas DataFrames.
>DataFrame basics
Unlike RDDs, however, they keep track of schemas and facilitate relational operations as well as procedural operations such as map.
>RDD versus DataFrame

Similarities:
Both can handle disparate data sources
Both are lazily evaluated (execution happens when an output operation is performed on them), thereby having the ability to take the most optimized execution plan

Differences between DataFrames:
DataFrames are a higher-level abstraction than RDDs.
The definition of RDD implies defining a Directed Acyclic Graph (DAG) whereas defining a DataFrame leads to the creation of an Abstract Syntax Tree (AST). An AST will be utilized and optimized by the Spark SQL catalyst engine.
RDD is a general data structure abstraction whereas a DataFrame is a specialized data structure to deal with two-dimensional, 
table-like data.


4.Creating DataFrames 
Spark DataFrame creation is similar to RDD creation. To get access to the DataFrame API, you need SQLContext or HiveContext as an entry point.

>From RDDs
The following code creates an RDD from a list of colors followed by a collection of tuples containing the color name and its length. 
It creates a DataFrame using the toDF method to convert the RDD into a DataFrame. 
The toDF method takes a list of column labels as an optional argument:

#Create a list of colours
colors = ['white','green','yellow','red','brown','pink']
#Distribute a local collection to form an RDD
#Apply map function on that RDD to get another RDD containing colour, length tuples
color_df = sc.parallelize(colors).map(lambda x:(x,len(x))).toDF(["color","length"])
#DataFrame[color: string, length: bigint]
color_df.dtypes  #Note the implicit type inference
#[('color', 'string'), ('length', 'bigint')]
color_df.show()  #Final output as expected. Order need not be the same as shown

color_df = sc.parallelize(colors).map(lambda x:(x,len(x))).toDF(["hello","length"])

We created an RDD here and then transformed that to tuples which are then sent to the toDF method. 
Note that toDF takes a list of tuples instead of scalar elements. 
You need to pass tuples even to create single-column DataFrames. 
Each tuple is akin to a row. You can optionally label the columns; 
otherwise, Spark creates obscure names such as _1, _2. Type inference of the columns happens implicitly.

>From JSON
#The Spark DataFrame API lets developers convert JSON objects into DataFrames and vice versa.
#Pass the source json data file path
df = sqlContext.read.json("./authors.json")
df.show() #json parsed; Column names and data types inferred implicitly
#Spark infers schemas automatically from the keys and creates a DataFrame accordingly.


>From JDBC sources 
#Launch shell with driver-class-path as a command line argument
pyspark --driver-class-path /usr/share/   java/mysql-connector-java.jar
#Pass the connection parameters
peopleDF = sqlContext.read.format('jdbc').options(
                           url = 'jdbc:mysql://localhost',
                           dbtable = 'test.people',
                           user = 'root',
                           password = 'mysql').load()
#Retrieve table data as a DataFrame
peopleDF.show()

>From other data sources
#Creating DataFrames from Apache Parquet
Apache Parquet is an efficient, compressed columnar data representation available to any project in the Hadoop ecosystem. 
Columnar data representations store data by column, as opposed to the traditional approach of storing data row by row. 
Use cases that require frequent querying of two to three columns from several columns benefit greatly from 
such an arrangement because columns are stored contiguously on the disk and you do not have to read unwanted columns in row-oriented storage. 
Another advantage is in compression. Data in a single column belongs to a single type. The values tend to be similar, and sometimes identical. 
These qualities greatly enhance compression and encoding efficiency. 
Parquet allows compression schemes to be specified on a per-column level and allows adding more encodings as they are invented and implemented.


#The following example writes the people data peopleDF loaded into a DataFrame in the previous example into Parquet format and 
#then re-reads it into an RDD:
#Write DataFrame contents into Parquet format
peopleDF.write.parquet('writers.parquet')
#Read Parquet data into another DataFrame
writersDF = sqlContext.read.parquet('writers.parquet')
#writersDF: org.apache.spark.sql.DataFrame = [first_name: last_name: string, gender: string, dob:    date, occupation: string, person_id: int]


DataFrame Operations:
Developers chain multiple operations to filter, transform, aggregate, and sort data in the DataFrames. 
The underlying Catalyst optimizer ensures efficient execution of these operations. 
These functions you find here are similar to those you commonly find in SQL operations on tables:


#Create a local collection of colors first
colors = ['white','green','yellow','red','brown','pink']
#Distribute the local collection to form an RDD
#Apply map function on that RDD to get another RDD containing colour, length tuples and convert that RDD to a DataFrame
color_df = sc.parallelize(colors).map(lambda x:(x,len(x))).toDF(['color','length'])
#Check the object type
color_df
#DataFrame[color: string, length: bigint]
#Check the schema
color_df.dtypes
#[('color', 'string'), ('length', 'bigint')]
#Check row count


 //List out column names
color_df.columns
#[u'color', u'length']
#Drop a column. The source DataFrame color_df remains the same. //Spark returns a new DataFrame which is being passed to show
color_df.drop('length').show()


//Convert to JSON format
color_df.toJSON().first()
'{"color":"white","length":5}'
#filter operation is similar to WHERE clause in SQL
#//You specify conditions to select only desired columns and rows
#//Output of filter operation is another DataFrame object that is usually passed on to some more operations
#//The following example selects the colors having a length of four or five only and label the column as "mid_length"
filter
------
color_df.filter(color_df.length.between(4,5)).select(color_df.color.alias("mid_length")).show()


//This example uses multiple filter criteria
color_df.filter(color_df.length > 4).filter(color_df[0]!="white").show()


//Sort the data on one or more columns
sort
----
#A simple single column sorting in default (ascending) order
color_df.sort("color").show()
#//First filter colors of length more than 4 and then sort on multiple columns
#/The Filtered rows are sorted first on the column length in default ascending order. 
# Rows with same length are sorted on color in descending order
color_df.filter(color_df['length']>=4).sort("length",'color',ascending=False).show()


//You can use orderBy instead, which is an alias to sort
color_df.orderBy('length','color').take(4)
#[Row(color=u'red', length=3), Row(color=u'pink', length=4),
#Row(color=u'brown', length=5), Row(color=u'green', length=5)]
#//Alternative syntax, for single or multiple columns.
color_df.sort(color_df.length.desc(),color_df.color.asc()).show()

//All the examples until now have been acting on one row at a time, filtering or transforming or reordering.
#The following example deals with regrouping the data
#These operations require "wide dependency" and often involve shuffling.
groupBy
-------
color_df.groupBy('length').count().show()

//Data often contains missing information or null values. We may want to drop such rows or replace with some filler information. 
#dropna is provided for dropping such rows
#The following json file has names of famous authors. Firstname data is missing in one row.
#dropna
------
df1 = sqlContext.read.json('./authors_missing.json')
df1.show()


//Let us drop the row with incomplete information
df2 = df1.dropna()
df2.show()  #Unwanted row is dropped


You already know by now that the DataFrame API is empowered by Spark SQL and that the Spark SQL's Catalyst optimizer plays a crucial role 
in optimizing the performance. Though the query is executed lazily, it uses the catalog component of Catalyst to identify whether 
the column names used in the program or expressions exist in the table being used and the data types are proper, 
and also takes many other such precautionary actions. The advantage to this approach is that, instead of waiting till program execution, 
an error pops up as soon as the user types an invalid expression.




Page 86


#Inbuilt Functions
#sc - Spark Context 
#take(n) Return an array with the first n elements of the dataset. 

#31 Apache Spark Core APIs - By Key Ranking - using flatMap
#Get Top N products by price with in each category - By Key Ranking - GroupByKey and flatMap
products = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/products")
productsFiltered = products.filter(lambda p: p.split(",")[4] != "")
for i in productsFiltered.take(100): print(i)


#37 Apache Spark Core APIs - Set Operations - Prepare Data
#Set operations - Prepare data - subsets of products for 2013-12 and 2014-01 
#Both the datasets have same identital data fields. Both the datasets will have all the attributes same. 
orders = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/orders")
orderItems = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/order_items")
#Apply map function on filter which will give us a fuple which contain order id as first field and order itself as second field.
orders201312 = orders.filter(lambda o: o.split(",")[1][:7] == "2013-12").map(lambda o: (int(o.split(",")[0]),o)) #File1 
for i in orders201312.take(10): print(i)
orders201401 = orders.filter(lambda o: o.split(",")[1][:7] == "2014-01").map(lambda o: (int(o.split(",")[0]),o)) #File1
for i in orders201401.take(10): print(i)

orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),oi)) #File2

orders201312Join = orders201312.join(orderItemsMap)
for i in orders201312Join.take(10): print(i)
orders201401Join = orders201401.join(orderItemsMap)
for i in orders201401Join.take(10): print(i)

orderItems201312 = orders201312.join(orderItemsMap).map(lambda oi: oi[1][1])
orderItems201401 = orders201401.join(orderItemsMap).map(lambda oi: oi[1][1])
for i in orderItems201312.take(10): print(i)
for i in orderItems201401.take(10): print(i)

#38 Apache Spark Core APIs - Set Operations - union
#Set operations - Union - Get product ids sold in 2013-12 and 2014001
products201312 = orderItems201312.map(lambda p: int(p.split(",")[2]))
for i in products201312.take(10): print(i)
products201401 = orderItems201401.map(lambda p: int(p.split(",")[2]))
for i in products201401.take(10): print(i)
allproducts = products201312.union(products201401)
for i in allproducts.take(20): print(i)
products201312.count()
products201401.count()
allproducts.count()
# When you perform union operation it will not give distinct records. 
# If you want distinct records from both the datasets after performing uninon operation use dintinct()
allproducts = products201312.union(products201401).distinct()
#This how we can Perform Union operations on two identical data sets. 

#39 Apache Spark Core APIs - Set Operations - intersect and minus/subtract
#This operation is nothing but intersection. 
#Set operations - Intersection - Get product ids sold in both 2013-12 and 2014-01
products201312 = orderItems201312.map(lambda p: int(p.split(",")[2]))
for i in products201312.take(10): print(i)
products201401 = orderItems201401.map(lambda p: int(p.split(",")[2]))
for i in products201401.take(10): print(i)

commonproducts = products201312.intersection(products201401)
for i in commonproducts.take(20): print(i)

#With intersection the distinct is implicit. It will apply the dintinct automatically for us. 

#Minus Operation - Not available in APIs. API Name is subtract. 
#Set Operations - minus - Get product ids sold in 2013-12 but not in 2014-01 
product201312only = products201312.subtract(products201401)
for i in product201312only.take(10): print(i)
#Subtract doesn't apply distinct. So you can apply distinct if you want to day distinct. 
product201312only = products201312.subtract(products201401).distinct()
product201401only = products201401.subtract(products201312).distinct()
for i in product201401only.take(10): print(i)
productsSoldOnlyInOneMonth = product201312only.union(product201401only)

#40 Apache Spark Core APIs - Saving data in text file format
#saveAsTextFile(path) 	Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem,
#  HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.
#saveAsSequenceFile(path) Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, 
# HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. 
# In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, 
# Double, String, etc).
#saveAsObjectFile(path)  Write the elements of the dataset in a simple format using Java serialization, 
# which can then be loaded using SparkContext.objectFile(). 
#Typically we use saveAsTextFile on RDD. Spark have other set of apis such as json,avro etc. we will see later. 

#Saving as text files with delimeters - revenue per order id
orderItems = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
#Use orderid as key 
from operator import add
revenuePerOrderId = orderItemsMap.reduceByKey(add)
for i in revenuePerOrderId.take(10): print(i)

#If we save the file now each data line will be saved as it is as the data as shown in the output.This will be a problem for downstream apps.
# So, we will format the data the way we want and then we use saveastext file. Apply map, define lambda fun to process every record into the format we want. 

revenuePerOrderId = orderItemsMap.reduceByKey(add).map(lambda r: str(r[0]) + "\t" + str(r[1]))
for i in revenuePerOrderId.take(100): print(i)

# logic - str(t[0]) + "\t" + str(t[1])
#SaveAstextFile is on the top of RDD. 
#In this spark version, the string is not rounded to 2 digits after decimal. So, check it. 

#Use SaveAsTextFile to write the data to HDFS. 
revenuePerOrderId.saveAsTextFile("/Users/srikapardhi/Documents/bigdata/BdProjects/revenuePerOrderId")
# for i in sc.textFile("/Users/srikapardhi/Documents/bigdata/BdProjects/revenuePerOrderId").take(10): print(i) 
#Print this to check the saved file contents

#hadoop fs -ls "file location"
#hadoop fs -tail "file name to view the data"

#41 Apache Spark Core APIs - Saving data in text file format using compression
#Saving RDD back to HDFS
#Reusing the same code used in previous program. 
revenuePerOrderId = orderItemsMap.reduceByKey(add).map(lambda r: str(r[0]) + "\t" + str(r[1]))


# connect to cluster to check the configured compression codecs. Compression is related to filesystem and hence look at HDFS conf. 
cd /etc/hadoop/conf
vi core-site.xml and search for codecs 
Configuration : Gzipcodec, defaultcodec, Snappycodec , if not configured work with admin team and get it configured. 

revenuePerOrderId.saveAsTextFile("/Users/srikapardhi/Documents/bigdata/BdProjects/revenuePerOrderId_Compressed", compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")

#java.lang.RuntimeException: native snappy library not available: this version of libhadoop was built without snappy support. 
#Exception will be thrown as there is no hadoop installed.Else file will be saved and we can access the file using sc.textFile. 

#42 Apache Spark Core APIs - Saving data in different file formats - overview using json
#3rd party plugin from databricks for avro format. Supported file formats. orc,json,parquet,avro. Make sure RDD is convered to DF(Data frame)
#Saving as JSON - Get revenue per order id
orderItems = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap.reduceByKey(add).map(lambda r: (r[0], round(r[1], 2)))
#Now data is in the form of tupples. To save as file format , we need to convert RDD to DF.
revenuePerOrderIdDF = revenuePerOrderId.toDF(schema=["order_id","order_revenue"]).show()
#help(revenuePerOrderIdDF), save(path, format which the file should be saved)
revenuePerOrderIdDF.save("/Users/srikapardhi/Documents/bigdata/BdProjects/revenue_per_order_json", "json")
#help(revenuePerOrderIdDF.write())
revenuePerOrderIdDF.write.json("/Users/srikapardhi/Documents/bigdata/BdProjects/revenue_per_order_json", "json")
#To verify if we are able to read data or not from the loc saved. 
sqlcontext.read.json("/Users/srikapardhi/Documents/bigdata/BdProjects/revenue_per_order_json").show()

#When data is saved in these file formats we dont need to worry about delimiters. Because they will have meta data linked. But in text we need to take care of delimiters.



#43 Apache Spark Core APIs - Get daily revenue per product - Problem Statement

#44 Apache Spark Core APIs - Get daily revenue per product - Launching pyspark
#Resource Manager IP address: 8088 IP 
cd /etc/hadoop/conf 
vi yarn site.xml - Resource Manager Web Address - resourcemanager.webapp.address
Check Memory Total and Cores total. 
disk usage - du -- for checking the file sizes
du /data/retail_db/products 
du -s -h /data/retail_db/products
hadoop fs -du -s -h /data/retail_db/orders
hadoop fs -du -s -h /data/retail_db/order_items 
Number of Nodes, from resouce manager. 

Specify memory and executers through argument. Spark shell can be launched using the below command. 

pyspark --master yarn --conf spark.ui.port=12569 --num-executers 2 --executor-memory 512M 

//Saprk ui port not required in certification unless they ask explicitely.
if you are unsure about properties, we can check here
spark-submit 
most of the control arguments available with spark are available with spark-submit as well.


#Filter - We should first focusing on data immediately after reading the data. 
#Extract Key and Value pairs
#Joins, Aggregations. 

#Run spark with required configuration if explicitely given. 
pyspark --master yarn --conf spark.ui.port=12569 --num-executors 2 --executor-memory 512M 

#Read orders and OrderItems 
orders = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/orders")
for i in orders.take(10): print(i)
orders.count()

orderItems = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/order_items")
for i in orderItems.take(10): print(i)
orderItems.count()

#Filter for Completed and Closed Orders 
#for i in orders.map(lambda o: o.split(",")[3]).distinct().collect(): print(i)
#Collect will convert RDD to Array 
ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE", "CLOSED"])
for i in ordersFiltered.take(10): print(i)
ordersFiltered.count() 

#Join order and order_items 
# Each element needs to be paired RDD, that means tuple. 
ordersMap = ordersFiltered.map(lambda o:(int(o.split(",")[0]), o.split(",")[1]))
for i in ordersMap.take(10): print(i)
#OrdersMap - ID and Date from ordersmap

orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4]))))
for i in orderItemsMap.take(10): print(i)
#OrderItemsMap tuple : Product ID and sub total - Tuple. Make sure you have paired RDD before you join data.

#Join Data : Order ID, Date, Product ID and Order Items Subtotal.
ordersJoin = ordersMap.join(orderItemsMap)
for i in ordersJoin.take(10): print(i)

#(24, ('2013-07-25 00:00:00.0', (403, 129.99))) -> (('2013-07-25 00:00:00.0',403), 129.99)
#For each element in ordersJoin convert/transform- > Key has to be tuple and value needs to be 129.99.
# Hence we Use Map again to convert our join output so that we can pass it as input to reducebyKey to get revenue for each product. 

ordersJoinMap = ordersJoin.map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))
for i in ordersJoinMap.take(10): print(i)

#Join tables
from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)
for i in dailyRevenuePerProductId.take(10): print(i)

#Now, get product name instead of product ID. 
#Load data for products into RDD to get the product name. 
#Products need to be read from local file system. Hence we can use typical python api.
#Use API to convert the data to a collection of records.

#Open is an API which takes string as path. Open opens file in read mode. 
productsRaw = open("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/products/part-00000").read().splitlines()
#Splitlines convert it string by string to a list. Process each line as separate element as a collection. 
#type(productsRaw) - will be of type list. 

#sc.textFile() will loads data from file to an rdd. 
#sc.parallelize() will convert a collection into rdd. 
products = sc.parallelize(productsRaw)
#Converted to RDD.

#Define Product ID as Key and Product Name as Value
#Convert to tuple
productsMap = products.map(lambda p: (int(p.split(",")[0]), p.split(",")[2]))
for i in productsMap.take(10): print(i)

#productID as Key and Name Value 
dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda r: (r[0][1], (r[0][0], r[1])))
for i in dailyRevenuePerProductIdMap.take(10): print(i)

#Product ID as Key and Rest of the data as value.
dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)
for i in dailyRevenuePerProductJoin.take(10): print(i)

#Ascending orderby date and decending orderby revenue for that key has to be composite. 

dailyRevenuePerProduct = dailyRevenuePerProductJoin.map(lambda t: ((t[1][0][0], -t[1][0][1]), t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1]))
for i in dailyRevenuePerProduct.take(10): print(i)

dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
for i in dailyRevenuePerProductSorted.take(10): print(i)

dailyRevenuePerProductName = dailyRevenuePerProductSorted.map(lambda r: r[1])
for i in dailyRevenuePerProductName.take(10): print(i)

#Save the file into text format 
dailyRevenuePerProductName.saveAsTextFile("/Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python")

#Date Validation by sc.textFile("/Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python/").take(10)
#Hadoop fs commands are another way to validate.
hadoop fs -ls /Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python/
hadoop fs -cat /Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python/ -- Will print all the data 
hadoop fs -tail /Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python/part-00000 -- can only be used in text format. if its in spl file format will see various characters.

#Problem statemnt says save exactly in two files then use coalesce. 
dailyRevenuePerProductName.coalesce(2).saveAsTextFile("/Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python")

#Cleanup
hadoop fs rm -R /Users/srikapardhi/Documents/bigdata/BdProjects/daily_revenue_txt_python/
Once cleaned up , launch pyspark once again. 

#51 Apache Spark Core APIs - Get daily revenue per product - Save as avro
#Save data in special format as avro. Convert my data to DF, use APIs on top of DF and save as avro. 

pyspark --master yarn --conf spark.ui.port=12890 --num-executors 2 --executor-memory 512M --packages com.databricks:spark-avro_2.10:2.0.1

#Read orders and OrderItems 
orders = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/orders")
for i in orders.take(10): print(i)
orders.count()

orderItems = sc.textFile("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/order_items")
for i in orderItems.take(10): print(i)
orderItems.count()

#Filter for Completed and Closed Orders 
#for i in orders.map(lambda o: o.split(",")[3]).distinct().collect(): print(i)
#Collect will convert RDD to Array 
ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE", "CLOSED"])
for i in ordersFiltered.take(10): print(i)
ordersFiltered.count() 

#Join order and order_items 
# Each element needs to be paired RDD, that means tuple. 
ordersMap = ordersFiltered.map(lambda o:(int(o.split(",")[0]), o.split(",")[1]))
for i in ordersMap.take(10): print(i)
#OrdersMap - ID and Date from ordersmap

orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4]))))
for i in orderItemsMap.take(10): print(i)
#OrderItemsMap tuple : Product ID and sub total - Tuple. Make sure you have paired RDD before you join data.

#Join Data : Order ID, Date, Product ID and Order Items Subtotal.
ordersJoin = ordersMap.join(orderItemsMap)
for i in ordersJoin.take(10): print(i)

#(24, ('2013-07-25 00:00:00.0', (403, 129.99))) -> (('2013-07-25 00:00:00.0',403), 129.99)
#For each element in ordersJoin convert/transform- > Key has to be tuple and value needs to be 129.99.
# Hence we Use Map again to convert our join output so that we can pass it as input to reducebyKey to get revenue for each product. 

ordersJoinMap = ordersJoin.map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))
for i in ordersJoinMap.take(10): print(i)

#Join tables
from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)
for i in dailyRevenuePerProductId.take(10): print(i)

#Now, get product name instead of product ID. 
#Load data for products into RDD to get the product name. 
#Products need to be read from local file system. Hence we can use typical python api.
#Use API to convert the data to a collection of records.

#Open is an API which takes string as path. Open opens file in read mode. 
productsRaw = open("/Users/srikapardhi/Documents/bigdata/data-master/retail_db/products/part-00000").read().splitlines()
#Splitlines convert it string by string to a list. Process each line as separate element as a collection. 
#type(productsRaw) - will be of type list. 

#sc.textFile() will loads data from file to an rdd. 
#sc.parallelize() will convert a collection into rdd. 
products = sc.parallelize(productsRaw)
#Converted to RDD.

#Define Product ID as Key and Product Name as Value
#Convert to tuple
productsMap = products.map(lambda p: (int(p.split(",")[0]), p.split(",")[2]))
for i in productsMap.take(10): print(i)

#productID as Key and Name Value 
dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda r: (r[0][1], (r[0][0], r[1])))
for i in dailyRevenuePerProductIdMap.take(10): print(i)

#Product ID as Key and Rest of the data as value.
dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)

#Tuples, and not string concatinated 
dailyRevenuePerProduct = dailyRevenuePerProductJoin.map(lambda t: ((t[1][0][0], -t[1][0][1]), t[1][0][0], round(t[1][0][1]), 2), t[1][1]))

#Create RDD with tuple of 3 elements

dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
for i in dailyRevenuePerProductSorted.take(10): print(i)

dailyRevenuePerProductName = dailyRevenuePerProductSorted.map(lambda r: r[1])
for i in dailyRevenuePerProductName.take(10): print(i)

dailyRevenuePerProductNameDF =dailyRevenuePerProductName.coalesce(2).toDF(schema=["order_date", "revenue_per_product", "product_name"])
dailyRevenuePerProductNameDF.show()

#Save the file format 
dailyRevenuePerProductNameDF.save("path", "com.databricks.spark.avro")

#Validate 
sqlcontext.load("path", "com.databricks.spark.avro").show()

#Copy both from HDFS to local system. 
mkdir -p /home/Srikapardhi/daily_revenue_txt_python
cd daily_revenue_txt_python/
hadoop fs -help get -- two arguments - source target 
hadoop fs -get "File copy path" "Destination directort/."
validate by typing in hadoop ls -ltr 


#53 Apache Spark Core APIs - Get daily revenue per product - Develop as application
mkdir pythondemo
cd pythondemo/
mkdir retail
cd retail/
mkdir -p src/main/pthon 
cd src/main/python 
vi DailyRevenuePerProduct.py 

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Daily Revenue Per Product").setMaster("yarn-client")
# We are running in yarn client mode. Config object is created.
sc = SparkContext(conf=conf) 
#Code here - same as above

#Development is done on local machine, we need to run ans ship it to cluster. 

#Go to base directory of project and run spark submit.
cd retail
spark-submit --master yarn --conf spark.ui.port=12789 --num executors 2 --executor-memory 512M src/main/python/dailyRevenuePerProduct.py 

#take - used to validate of data is not of text file format when we validate it. 
#Test , validate, ship to cluster. Shell script is scheduled using cron or any other scheduling tool.

#Spark SQL 
#02 Spark SQL - Different Interfaces
# show databases;


#03 Spark SQL - Create Hive Tables - Text File Format


//14 Spark SQL - Sorting

//15 Spark SQL - Set Operations

Set operations are not very important. 

//16 Spark SQL - Analytics Functions - aggregations

Hive language Manul 
> Data Retrieval Queries:
1.The Windowing functions
2.The OVER clause 
3.Analytics functions 

//17 Spark SQL - Analytics Functions - ranking


//22 Spark SQL - Data Frame Operations

Data Frame Operations:
show
select
filter
join
And more


CCA 175 - Data Ingestion 
Streaming Analytics using Flume, Kafka and Spark Streaming using python.

//01 Streaming Analytics - Introduction

From webserver logs, or streaming data to do data ingesiton.
Spark Streaming Alternatives : Flink, Storm etc. 
Flume and Kafka are alternatives at times.
They are primararily to get data from logs.
Flink and Storm are not part of certification. 

//Apache Flume:
Flume is a distributed, reliable, and available service for efficiently collecting, aggregating, and moving large amounts of log data. 
It has a simple and flexible architecture based on streaming data flows. It is robust and 
fault tolerant with tunable reliability mechanisms and many failover and recovery mechanisms. 
It uses a simple extensible data model that allows for online analytic application.


Inject Data to HDFS using Flume
Different implementations on Flume
Next Kafka and then integration of Flume and Kafka and Kafka and Spark Streaming.
Kafka provides something called publisher API, to push data into kafka topic which can be used by spark. 

-- As of today, there are not many questions in this area.
Hardly one question that too very few people.
Kafka and Flume idea will help to get projects. 

//02 Streaming Analytics - Flume - Documentation and simple example
Apache Flume: 
Documentation : https://flume.apache.org/FlumeUserGuide.html
1.6.0 is the most releant one.
No major Differences between 1.6.0 and 1.8.0

Flume always will be running as an agent. 
Agent is a combination of Source , channel and sink.
Data will be streamed to channel. Quite often memory or file channel.
Primary responsibility of sink is to read data from channel and push the data into target. 

Each and every agent will have properties. 

Source will be typically configured to read data from web server or any place where we actually get data.

Sink will be typically configured to write data.
Channel will sync source and sink.

mkdir flume_demo
cd flume_demo
vi example.conf

-- Example from Flume site. 

# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444

# Describe the sink
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

--
type : is nothing but netcat, netcat is a webserver simulator 

sink : sink type is logger. Whenever messages are coming to port 
As it is webserve you can telnet into it.
Using combination of ip address and localhost you can put some data.

Flume agents are started using flume-ng 
#How to run an agent and pass conf file path. 
flume-ng agent --name a1 --conf-file path/example.conf   

#When agent is started you should watch there are no exceptions raised.
created serversocket on localhost and port.
netcat is purely for exploration purposes.

Sink is nothing but the logger. 

When we send messages those msgs will come to sink. 

telnet localhost 44444

Hello World
How are you?

Messages - will be shown on the screen of sink as it is configured as logger.

//03 Streaming Analytics - Flume - Get data from web server logs to HDFS - Introduction

Flume Sources:
Supports several sources.
Avro
thrift - REST APIs
Exec - Web server logs. 
JMS 

common property is type. 

exec - is to primararily issue a command on to a file to get data in streaming fashion. 
tail -F [file] : So that flume as an agent keep on reading the data at regular intervals.
exec as a source. 

Flume Sinks:
HDFS Sink
Hive sink
Logger Sink
AVRO Sink 
Thrift Sink 
IRC Sink
Null Sink

For Certification : HDFS sink - Write events to HDFS system. 

The main property after type is hdfs.path


Flume Channels:
Channel will actually queue up the data into micro batches.
And the batch of data will be passed on at regular intervals.

Memory Channel
JDBC Channel
File Channel
Kafka Channel

Certification : Memory Channel/File Channel

File channel will have more capacity. Lot more reliable but slower compared to memory. 

//04 Streaming Analytics - Flume - Setting up data

Get Data from web server logs to HDFS
Source : exec
Sink : HDFS 
Channel : Memory 

Cloudera gives a very good web logs stimulator called gen logs. 
#Logs are deployed in this location on cloudera. 
cd /opt/gen_logs/
ls -ltr  

tail will just preview if data is streamed. 
tail -s using exec as source. 

tail -F /opt/gen_logs/logs/access.log 

using excec - as part of flume agent source with type exced. 

We will be capturing data in streaming fashion and push that data to HDFS using memory channel.

//05 Streaming Analytics - Flume - Web Server logs to HDFS - Defining Source

Flume demo directory
mkdir wslogstohdfs 
cp example.conf wslogstohdfs/
cd wslogstohdfs/
mv example.conf wshdfs.conf

#Change Confs
agent name  : a1 to wh 
source name : r1 to ws
channel name: c1 to mem 

sink we will keep as k1 for now.
Only changes to source.

Source type = exec 
bind and port are not relevant when source type is exec. 

wh.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

#Now start flume agent
flume-ng agent -n wh -f /home/srikapardhi/flume_demo/wslogstohdfs/wshdfs.conf

#It is actually printing the binary value of the log file.

changed the source to exec.


//06 Streaming Analytics - Flume - Web Server logs to HDFS - Sink - Introduction

Source : exec 
Sink : hd
Channel : Memory

Now, change the sink to HDFS.
Rename sink k1 to HDFS

Sink 
type : hdfs 
wh.sinks.hd.type = hdfs
wh.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/srikapardhi/flume_demo
#Full path needs to be given. 
#URL from where we got the URL

hadoop fs -rm -R /user/srikapardhi/flume_demo 
cd etc/properties/hadoop/conf
vi core-site.xml 
fs.defaultFS value is the suffix. 

#Going back to the property file 
~/flume_demo/wslogstohdfs

run the flume agent command which we ran earlier. 

hadoop fs -ls /user/srikapardhi/flume_demo
#Defaults it takes as we haven't configured anything.

File prefix is by default Flume Data + appends Unix Time stamp.

inuse file also you can define prefix and sufix names.
inuse file is that where flume still hasn't finished writing the file completely.
tmp file will be converted to main file without tmp once finished.

roll interval : 30 , seconds it will roll  a new file. 
roll count : the threshold with respect to no. of messages. 
roll size : default is 1KB. until the size is reached it will create a file. 

Once threshold is reached, tmp file will be reached to permanent file.
File type is bydefault a SequenceFile so we are seeing binary files.

//07 Streaming Analytics - Flume - Web Server logs to HDFS - Customize Sink

#Changing the conf file with properties. 

wh agent name
hd is sink name 

#Conf 
wh.sink.hd.hdfs.filePrefix = FlumeDemo
wh.sink.hd.hdfs.fileSuffix = .txt 
wh.sink.hd.hdfs.rollSize = 1048576
wh.sink.hd.hdfs.rollInterval = 120
wh.sink.hd.hdfs.rollCount = 100 // default is 10 
wh.sink.hd.hdfs.fileType = DataStream // by default is sequence file

#Now start running the agent. 
flume-ng agent 

#Validate :
hadoop fs -ls /path/
hadoop fs -cat /file name/ 
hadoop fs -cat /file name/|wc -l // number of messages can be seen. 


//08 Streaming Analytics - Flume - Web Server logs to HDFS - memory channel

Channel : memory 

Memory is transient. If the agent fails, data in the channel will be lost.
Typically memory in the channel will be configured to send as early as possible.

capacity = maximum no. of events stored in the channel.
By that time messages are not consumed it will start throwing errors.

transactionCapacity : Will be the max capacity or no of events the data can be revenuePerOrderId_Compressedper transaction per source or sink
typically capacity will be higher and transactionCapacity will be bit lower.

Probably, transactionCapacity value can be half of the capacity.
or less than the value

What are the different models which can be used while configuring the agents.

//09 Streaming Analytics - Flume - Different implementations of agents

#Flume architecture
Agent will have 3 components
Source : Will read data from source
Channel : communicate between source and sink in a reliable and consistent fashion.
Sink : Writing data to some sort of data : hdfs 

#Ambari and check installation of flume.
Flume will have no daemon process that will be running.

Whenever we launch flume, we will be launching on gateway node.
But just to deploy the jobs.

cd flume_demo/
cd wslogstohdfs/
set -o vi

#different implementations of flume agents.
1.Multi Agent : Output of one agent will be input of one agent. 
Typically this is done by Avro RPC calls.It is a liner flow. 

2.Consolidation : Typical scenario. Multiple web servers or application servers. 
load balancing, scalability, fault tolerance.
When data needs to be read from multiple servers from different agents. 
we use consolidation implementation.
Even this is usecase for multiagent flow.

3. Multiplexing the flow :
One source and try to write data to multiple sinks.
This is called multiplexing.

We will use Multiplexing to save data to hdfs or spark streaming.

Two types: 
1.Raw data to HDFS directly. backup. we can reconsile easily. 
2.We will stream to Spark streaming and that will be process the data 
and store it back to HDFS. 

#Whenever there is a sink, there should be an associated channel to it.

Our example: 
2 Sinks
2 channels
1 source - web server logs 
Multiplexing extensively used in real world scenarios.

//10 Streaming Analytics - Kafka - High level architecture

As part of certification not many questions are asked.
Emphasis will be more on spark than on kafka.
But for projects you need to refer documentation.
Current Version : 2.0 
Documentation : https://kafka.apache.org/documentation/

#Producers - Kafka Broker - Consumers 

Typically producers are web applications. 
Producer API : we will use to capture the data/messages.

Subscribe to that kafka cluster and downstream application will consume the data.
Downstream apps like analytics or any apps.

#Topic : Topic in the cluster. This is what captures the data.

//11 Streaming Analytics - Kafka - Using commands

1.Creating the topic : We need to use a component called Zookeeper. 
2.Publish the messages /Read the messages 

Create/drop topics - we need Zookeeper component.

--
Kafka uses Zookeeper for the following:

Electing a controller. The controller is one of the brokers and is responsible for maintaining the leader/follower relationship for all the partitions. When a node shuts down, it is the controller that tells other replicas to become partition leaders to replace the partition leaders on the node that is going away. Zookeeper is used to elect a controller, make sure there is only one and elect a new one it if it crashes.

Cluster membership - which brokers are alive and part of the cluster? this is also managed through ZooKeeper.

Topic configuration - which topics exist, how many partitions each has, where are the replicas, who is the preferred leader, what configuration overrides are set for each topic

(0.9.0) - Quotas - how much data is each client allowed to read and write

(0.9.0) - ACLs - who is allowed to read and write to which topic (old high level consumer) - Which consumer groups exist, who are their members and what is the latest offset each group got from each partition.
--


#ip port no for nodes - server hosts 
Ambari > Zookeeper > Configs > 
:PORT NO

#These are all shell commands to learn and test kafka. 
In real world apps, we don't do these.
We will use plugins to create all the topics and messages.


cd /usr/hdp/current/kafka-broker/
cd /usr/hdp/current/kafka-broker/bin
ls -ltr
pwd
export PATH=$PATH:  
#~ = represents tilda 
vi ~/.bash_profile 
cd 
vi .bash_profile 

. ~/.bash_profile 
exit and relogin again.
type kafka and hit tab : you should be able to see all the commands.

Topics can be created with the support of Zookeeper.

#Create a Topic:
kafka-topics.sh --create 
--zookeeper localhost:2181 
--replication-factor 1 
--partitions 1 
--topic topicname

#Zookeeper Required
#Validation of Topic Creation:
kafka-topics.sh --list --zookeeper localhost:2181 --topic topicname
kafka-topics.sh --list --zookeeper localhost:2181 // Will show all topics 

#Produce Messages:
kafka-console-producer.sh --broker-list localhost:9092 --topic topicname

#Consumer: 
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topicname --from-beginning
kafka-console-consumer.sh --zookeeper localhost:9092 --topic topicname --from-beginning //previous version

#from messages 
--from-beginning
--from-now 

#The topic : 
create
list
produce 
consumer 
describe 
delete 

//12 Streaming Analytics - Kafka - Anatomy of topic

--partitions is primarily for scalability purposes.
1 = A file will be created in only 1 node.
2 = Two files will be created in 2 nodes. 

Partition : is for scalability
Replication : is for Availability 

High Availability, Load balancing and scalability at high level.

offset - offset is the position of the last message which consumer has read. 

//13 Streaming Analytics - Role of Kafka and flume

Flume and Kafka - Primarily to get data from source.

Flume: 
Main advantage of flume is to get the data from the logs which are already getting the messages. 
If source application is already publishing messages to web server logs, 
we can just read the logsby deploying flume agent. Deploy flume agent and get the logs. 

#Flip Side:
Not reliable
Scalibility 
More the No. of sinks, managing the flume is difficult.

Kafka fills this gap.
Kafka - beats flume on realiability and scalability.
#Disadvantages:
Kafka can't be used if there is an application already sending logs. 
code needs to be refractored to get the topics. 

Flume can be set up to read the messages from existing log files and the messages can be pushed to kafka by flume.
As the msgs are pushed to kafka by flume we can use topic subscribe.
If its a new application we can directly go with kafka.We don't need to think of flume at all.

#Spark Streaming
//14 Spark Streaming - Getting Started
1.SparkContext (Web Service)
2.SQLContext(Wrapper on top of SparkContext)
3.StreamingContext 

#spark-shell 
it will import spark based binaries, it is actually creating web service for user.
sc : spark context
sqlContext : to read data from files or database. process it and then save them whereever required.
Read process and load the data at less frequent jobs. 
Even if it is possible we should not use sqlContext for less frequent jobs.

spark context and streaming context can't run at the same time.
Either spark context or streaming context.


import org.apache.spark.streaming [hit tab] 


import org.apache.spark.SparkConf
val conf = new SparkConf().setAppName("streaming").setMaster("yarn-client")
val ssc = new StreamingContext(conf, )

Spark-shell with streaming context is a bit buggy.
Hence will go application specific approach. 

SparkContext : Read, process and save the data at a Low frequency interval
StreamingContext : Read, process and save the data at a Very high frequency interval, as low as 5sec or less. But very rare to go less than 10sec. 

//15 Spark Streaming - Setting up netcat

Need to have a web service, the streaming context can poll the data.
#MacOS
nc - netcat 
nc -lk 9999

#Windows 
netcat on windows 
Download the zip, set the env variables. Update the system path. 

nc -l -p 9999

//16 Spark Streaming - Develop Streaming word count

Develop the code
Start the netcat service
Run the application using spark submit

pwd 
StreamingContextdemo.py

#Application
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("streaming word count").setMaster("yarn-client")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 15) 
#Takes two arguments. First sc, and the frequency at which the program should process the data. 
#Read the data, process the data or write it back to the db or file. 
lines = ssc.socketTextStream("ipaddress", 19999)
#Dstream datatype of lines
words = lines.flatMap(lambda line: line.split(" "))
wordTuples = words.map(lambda word: (word,1))
wordCount = wordTuples.reduceByKey(lambda x,y: x+y)
wordCount.pprint()

ssc.start()
ssc.awaitTermination()


//17 Spark Streaming - Run on the cluster
nc -lk ip address 19999
#Ready to accept the messages 

#Start the spark streaming app using spark-submit. 

spark-submit --master yarn --conf spark.ui.port=12890 src/main/python/StreamingwordCount.py 
#This will run streaming context not just spark context. 

#Integration points, ip address and port numbers - points where people struggle.

//18 Spark Streaming - Data Structure (DStream) and APIs

DStream and APIs

Discretized Streams (DStreams)
Every 10 seconds it will create an RDD. every RDD is a new RDD.
The stream of RDDs over a period of time at regular intervals.
This concept of RDD generated at regular intervals is called discretized. 

map, flatMap, filter, reducebyKey, join, Update, reduce, count - same APIs. 

Window Operations : Additional Transformations. 
Series of Independent or isolated RDDs. 

30min window at every 10min intervals. 
Any timeseries windows, then we need to use window APIs. 

RDD set at frequent intervals and the APIs on the top of those streams. 

//19 Spark Streaming - Department Wise Count - Problem Statement 

gen_logs : application that will stimuate log files. 

ls -ltr /opt/gen_logs/logs/

tail_logs.sh
start_logs.sh

>Spark Streaming context 
>filter :
>map : Extract Department name, 
>reduceByKey 
>output operations : saveAsTextFile - save as output to the file system


//20 Spark Streaming - Streaming department wise traffic - Develop

StreamingDepartmentCount.py

#Code

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import sys 
hostname = sys.argv[1]
port = int(sys.argv[2])

conf = SparkConf().setAppName("StreamingDepartmentCount").setMaster("yarn-master")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

messages = ssc.socketTextStream(hostname, port)
departmentMessages = messages.filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department")
departmentNames = departmentMessages.map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

from operator import add
departmentCount = departmentNames.reduceByKey(add)

outputPrefix = sys.argv[3]
departmentCount.saveAsTextFiles(outputPrefix)
ssc.start()
ssc.awaitTermination()

//21 Spark Streaming - Streaming department wise traffic - Run and validate

tail_logs.sh | nc -lk node address 19999 
#get from tail and keep on piping into ip address and port number. 

spark-submit --master yarn --conf spark.ui.port = 12890 src/main/python/StreamingDepartmentCount.py hostname portnumber outputpath/cnt 

hadoop fs -ls /user/path/

//22 Flume and Spark Streaming - Department Wise Count - Setup Flume Agent

sdc.channels = hdmem sparkmem

location of jar files: 
cd /usr/hdp/
/flume/lib/
ls -ltr 


jar needs to be added:
spark-streaming-flume-sink
commons-lang3


#Spark 1.8.3 and flume integration. 
Spark Streaming + Flume Integration Guide 
1. Push based approach
2. Pull based approach - custom sink : plugin provided by spark itself. 
#Cloudera need to figure out where the libs are and then make sure the jars are present. 
you need to add jars in flume library. 

agent name : sdc 
spark sink class : fully classified name 

flume-ng agent -n sdc -f sdc.conf 
#Code in OneNote

Flume agent will start, then data will be pushed to hdfs as well as the spark sink. 

//24 Flume and Spark Streaming - Department Wise Count - Develop application

#Code

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

import sys 
hostname = sys.argv[1]
port = int(sys.argv[2])

conf = SparkConf().setAppName("StreamingDepartmentCount").setMaster("yarn-master")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

agents = [(hostname, port)]
pollingStream = FlumeUtils.createPollingStream(ssc, agents)
messages = pollingStream.map(lambda msg: msg[1])
departmentMessages = messages.filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department")
departmentNames = departmentMessages.map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

from operator import add
departmentCount = departmentNames.reduceByKey(add)

outputPrefix = sys.argv[3]
departmentCount.saveAsTextFiles(outputPrefix)
ssc.start()
ssc.awaitTermination()


//25 Flume and Spark Streaming - Department Wise Count - Run on cluster

#Run and Validate on the cluster 

Start flume agent > run spark submit with python program > validate whether files are generated or not. 

#Start the flume agent 
flume-ng agent -n sdc -f sdc.conf 

#Run from base directory of our application 
spark-submit --master yarn --conf spark.ui.port=12890 --jars "fully qualified jar names" src/main/python/streamingFlumeDepartmentCount.py hostname port /user/srikapardhi/streamingflumedepartmentcount/cnt

#Hadoop
hadoop fs -ls /user/srikapardhi/streamingflumedepartmentcount
hadoop fs -ls /user/srikapardhi/streamingflumedepartmentcount/cnt /part*
#We can see contents of the file 

//Spark streaming flume sink and spark streaming flume 
Make sure, pre download jars during run time.


//26 Flume and Kafka integration - Create config file

#Integrating flume and kafka.
Prefer Kafka in most of the cases as long as you can refractor your code.
Kafka gives us scalability and reliability etc.
If you have legacy application, instead of refractoring the code use flume.
Flume - read logs. Push logs of legacy apps to kafka to a topic group.
Typical approach of creating data pipelines. 
process it using spark streaming, storm or flink. 


cd flume_demo/

#Check flume version using the below command 
#flume-ng version

#Code
#wskafka.conf: A single-node Flume configuration to read
#data from webserver logs and publish to kafka topic

#Name the components on this agent 
wk.sources = ws
wk.sinks = kafka 
wk.channels = mem 

#Describe/configure the source 
wk.sources.wk.type = exec 
wk.sources.ws.command = tail -F /opt/gen_logs/log/access.log

#Describe the sink
wk.sinks.kafka.type = org.apache.flume.sink.kafka.KafkaSink 
#If its ambari go to ambari manager and get information about brokerlist.
>Kafka > Configs > go to broker hosts >  listeners Number is port number. 
#If its cloudera go to cloudera manager. 
wk.sinks.kafka.brokerList = hostname:port, hostname:port, hostname:port
wl.sinks.kafka.topic =  fkdemosrik
#If you don't give the topic name it will take defualt topic. 

#use a channel wkich buffers events in memory 
wk.channel.mem.type = memory  
wk.channel.mem.capacity = 1000
wk.channel.mem.transactionCapacity = 100

#Bind the source and sink to the channel 
wk.sources.ws.channels = mem
wk.sinks.kafka.channel = mem 


Write to kafka topic using memory channel.

//27 Flume and Kafka integration - Run and validate

#Run - by running flume
flume-ng agent --name wk --conf-file /fully qualified path/wskafka.conf 

#Validate by running kafka consumer command 
#history|grep consume 

kafka-console-consumer.sh --zookeeper hostname:port, hostname:port --topic fkdemosrik --from-beginning

#Ideally we will use app to use this data like spark streaming, storm or flink. 

#Check for logs and go to log directory , log path can be found on ambari or cloudera manager. 
#Better way of validating is by using kafka console consumer.sh 

//29 Kafka and Spark Streaming - Department Wise Count - Develop and build application

Approach 2 : Direct Approach (No Receivers) - Everything is managed by kafka itself 
Spark + kafka integration guide 

#Code 
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import sys 

conf = SparkConf().setAppName("Streaming Department Count").setMaster("yarn-master")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

topics = ["fkdemosrik"]
brokerList = {"metadata.broker.list" : hostname:port, hostname:port}
#directkafkaStream = KafkaUtils.createDirectStream(ssc, [topics], (brokerList))
directkafkaStream = KafkaUtils.createDirectStream(ssc, topics, brokerList)

messages = directkafkaStream.map(lambda msg: msg[1])
departmentMessages = messages.filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department")
departmentNames = departmentMessages.map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

from operator import add
departmentCount = departmentNames.reduceByKey(add)

outputPrefix = sys.argv[1]
departmentCount.saveAsTextFiles(outputPrefix)
ssc.start()
ssc.awaitTermination()

#There is no easy way to validate these. So follow the entire procedure. 

//30 Kafka and Spark Streaming - Department Wise Count - Run and validate application 

#Start flume agent and then comeup with spark submit and then run the application. 

flume-ng agent -n wk -f wskafka.conf 

#spark-submit command 
spark-submit --master yarn --conf spark.ui.port=12890 --jars "fully qualified path names/libs/name.jar, /path/, /path/.jar" src/main/python/StreamingKafkaDepartmentCount.py /user/srikapardhi/streamingkafkadepartmentcount/cnt 

#jars is preferred than packages. During run time use config parser to read config files during production. 
#Will launch Spark streaming context and will process data at every regular intervals we configured

#Validate using another session
hadoop fs -ls /user/srikapardhi/streamingkakfadepartmentcount
#Once data is coming 
hadoop fs -cat /filename/part* 


@CCA175 Spark and Hadoop Developer Certification

CCA175 is a hands-on, practical exam using Cloudera technologies. 
Each user is given their own CDH5 (currently 5.15.0) cluster pre-loaded with 
Spark 1.6, Spark 2.3, Impala, Crunch, Hive, Pig, Sqoop, Kafka, Flume, Kite, Hue, Oozie, DataFu, 
and many others. 
Full List can be checked here : https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_vd_cdh_package_tarball_510.html
In addition the cluster also comes with Python (2.6, 2.7, and 3.4), 
Perl 5.10, Elephant Bird, Cascading 2.6, Brickhouse, Hive Swarm, 
Scala 2.11, Scalding, IDEA, Sublime, Eclipse, and NetBeans.

#Certification Exam Pattern:

#Precautions During the Exam:
Mozilla Browser : Ctrl + : Zooms in the text size of the browser that allows you to read the browser. 
terminal : Ctrl Shift + : Zoom in the text 

Problem 10-12. 
You can attempt the questions in any order. Your submission is graded when the exam is complete. So, 
you may change your solutions at any time. Please read output instructions carefully. Pay particular attention to the Data Description
and output requirements sections. Probably good to revist the output requirements after completing to verify each problem.

#Data Ingest
#The skills to transfer data between external systems and your cluster. This includes the following:
1.Import data from a MySQL database into HDFS using Sqoop
Connect to the MySQL database on the cluster using sqoop and import all of the data from the customer table into HDFS. 
Given:
#MySQL database information:
Installation: On the cluster node gateway 
Database Name: problem1
Table Name: customer
Username: cloudera 
Password: cloudera 

#Output Requirements
1.Place the customer files in the HDFS Directory
/user/cert/problem1/solution/
2.use a text format with a comma as the columnar delimiter
3.Load every customer record completely

Solution:

sqoop import --connect jdbc:mysql://gateway/problem1 \
--table customer --username cloudera --password cloudera \
--target-dir /user/cert/problem1/solution/ 

Text and comma are sqoop defaults. 
So, no need to add any config.
Also, there is no requirement for WHERE clause as we are used to load complete data. 







2.Export data to a MySQL database from HDFS using Sqoop
3.Change the delimiter and file format of data during import using Sqoop
4.Ingest real-time and near-real-time streaming data into HDFS
5.Process streaming data as it is loaded onto the cluster
6.Load data into and out of HDFS using the Hadoop File System commands
#Transform, Stage, and Store
#Convert a set of data values in a given format stored in HDFS into new data values or a new data format and write them into HDFS.
1.Load RDD data from HDFS for use in Spark applications
2.Write the results from an RDD back into HDFS using Spark
3.Read and write files in a variety of file formats
4.Perform standard extract, transform, load (ETL) processes on data
#Data Analysis
#Use Spark SQL to interact with the metastore programmatically in your applications. 
# Generate reports by using queries against loaded data.
1.Use metastore tables as an input source or an output sink for Spark applications
2.Understand the fundamentals of querying datasets in Spark
3.Filter data using Spark
4.Write queries that calculate aggregate statistics
5.Join disparate datasets using Spark
6.Produce ranked or sorted data
#Configuration
This is a practical exam and the candidate should be familiar with all aspects of generating a result, not just writing code.
1.Supply command-line options to change your application configuration, such as increasing available memory


--
Cloudera Official Training Topics :
#Course Outline
#Introduction to Apache Hadoop and the Hadoop Ecosystem
Apache Hadoop Overview
Data Ingestion and Storage
Data Processing
Data Analysis and Exploration
Other Ecosystem Tools
Introduction to the Hands-On Exercises
#Apache Hadoop File Storage
Apache Hadoop Cluster Components
HDFS Architecture
Using HDFS
#Distributed Processing on an Apache Hadoop Cluster
YARN Architecture
Working With YARN
#Apache Spark Basics
What is Apache Spark?
Starting the Spark Shell
Using the Spark Shell
Getting Started with Datasets and DataFrames
DataFrame Operations
#Working with DataFrames and Schemas
Creating DataFrames from Data Sources
Saving DataFrames to Data Sources
DataFrame Schemas
Eager and Lazy Execution
A#nalyzing Data with DataFrame Queries
Querying DataFrames Using Column Expressions
Grouping and Aggregation Queries
Joining DataFrames
#RDD Overview 
RDD Data Sources
Creating and Saving RDDs 
RDD Operations
#Transforming Data with RDDs
Writing and Passing Transformation Functions 
Transformation Execution
Converting Between RDDs and DataFrames 
#Aggregating Data with Pair RDDs
Key-Value Pair RDDs 
Map-Reduce
Other Pair RDD Operations 
#Querying Tables and Views with Apache Spark SQL
Querying Tables in Spark Using SQL 
Querying Files and Views
The Catalog API 
Comparing Spark SQL, Apache Impala, and Apache Hive-on-Spark
#Working with Datasets in Scala
Datasets and DataFrames 
Creating Datasets
Loading and Saving Datasets 
Dataset Operations
#Writing, Configuring, and Running Apache Spark Applications
Writing a Spark Application 
Building and Running an Application
Application Deployment Mode 
The Spark Application Web UI
Configuring Application Properties 
#Distributed Processing
Review: Apache Spark on a Cluster
RDD Partitions
Example: Partitioning in Queries
Stages and Tasks
Job Execution Planning
Example: Catalyst Execution Plan
Example: RDD Execution Plan
#Distributed Data Persistence
DataFrame and Dataset Persistence
Persistence Storage Levels
Viewing Persisted RDDs
#Common Patterns in Apache Spark Data Processing
Common Apache Spark Use Cases
Iterative Algorithms in Apache Spark
Machine Learning
Example: k-means
#Apache Spark Streaming: Introduction to DStreams
Apache Spark Streaming Overview
Example: Streaming Request Count
DStreams
Developing Streaming Applications
#Apache Spark Streaming: Processing Multiple Batches
Multi-Batch Operations
Time Slicing
State Operations
Sliding Window Operations
Preview: Structured Streaming
#Apache Spark Streaming: Data Sources
Streaming Data Source Overview
Apache Flume and Apache Kafka Data Sources
Example: Using a Kafka Direct Data Source
--
