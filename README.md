# Py DB Cluster for distributed MySql
A Python project that manages a MySql cluster with distributed/parallelized queries and databases

## Overview

The parallel SQL processing system consists of a cluster of MySql instances running on different machines.
DDLs are translated into corresponding DDLs for each individual DBMS instance in the cluster and executed there.
In addition, a catalog database on the controller node stores metadata about what data is stored for each table on each DBMS instance in the cluster.

## Requirements

- [Python2.\*](https://www.python.org/)
- [pip](https://pypi.python.org/pypi/pip)
- Content in **requirements.txt**
- Root access to MySql server environments

## Installation

- Install the pip requirements onto your local machine or into a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/).

```bash
sudo pip install -r requirements.txt
```

## Usage

A configuration file is required for the script to gain access to the cluster.
The configuration file stores information like credentials, hostnames, database names, and partitioning methods.

Your configuration file should look something like the below example for a simple 2 node setup.
Note that even for a two node setup, a controller node is still required to administer the sql command across the cluster and store the catalog database.
Adjust this configuration file to suit your needs:

```
catalog.driver= com.mysql.jdbc.Drive
catalog.hostname=jdbc:mysql://127.0.0.1:3306/josephyl1
catalog.username=db2inst1
catalog.passwd=mypasswd

numnodes=2

node1.driver= com.mysql.jdbc.Drive
node1.hostname=jdbc:mysql://127.0.0.1:3306/josephyl2
node1.username=db2inst1
node1.passwd=mypasswd

node2.driver= com.mysql.jdbc.Drive
node2.hostname=jdbc:mysql://127.0.0.1:3306/josephyl3
node2.username=db2inst1
node2.passwd=mypasswd
```

The above example will orchestrate a distributed cluster with no intelligent sharding.
Currently, there is support for both a **hash based partition or a range based partition.**

### Hash Based Partitioning

A hash bashed partition will hash incoming insertions on a chosen column, and select a single node to carry that row.
The chosen column is hashed against the total number of nodes in the cluster for pseudo-random distribution.
For example, let's assume that we have a database of books where the ISBN of each book is stored.
To determine the node in the cluster that a book belongs to, the following formula is first executed:

```
target_node = ( book_isbn mod total_nodes ) + 1
```

In other words, for a book with the ISBN of 214323421 and a 2 node cluster:

```
target_node = (214323421 % 2) + 1
target_node = (1) + 1
target_node = 2
```

This particular book would be stored on node 2.
Due to the nature of hashing rows in this way, we can achieve pseudo random distribution.

Should you chose to use hash-based partitioning in your cluster, an example configuration file has been given in clustercfghash that looks like this:

```
catalog.driver= com.mysql.jdbc.Drive
catalog.hostname=jdbc:mysql://127.0.0.1:3306/josephyl1
catalog.username=db2inst1
catalog.passwd=mypasswd

tablename=books

partition.method=hash
partition.column=isbn
partition.param1=2
```

### Range Based Partitioning

Additionally, the project can also support a range based partition.
A range based partition will assign each node a range of values and the node will be responsible for storing rows that fall into the range.
For each node, a partparam1 and partparam2 is specified.
Continuing with our example of using books and ISBNs, partparam1 refers to the minimum value that an ISBN must be and partparam2 is the maximum value:

```
partparam1 < partcol <= partparam2
```

For our example lets assign partparam1 to be 100000000 and partparam2 to be 200000000 for node 1.
Assume that we are looking for the node that would carry a book with an ISBN value of 111111111.
The partition column is set to be ISBN, so the formula is:

```
100000000 < 111111111 <= 200000000
```

This holds true for node 1, so we know that the book with the ISBN value of 111111111 belongs to node 1.

A range based partition can offer slightly more flexibility depending on your preferences.
An example configuration file for a range based partition in a 2 node cluster is located at clustercfgrange that looks like this:

```
catalog.driver= com.mysql.jdbc.Drive
catalog.hostname=jdbc:mysql://127.0.0.1:3306/josephyl1
catalog.username=db2inst1
catalog.passwd=mypasswd

tablename=books
partition.method=range
partition.column=isbn

numnodes=2
partition.node1.param1=1
partition.node1.param2=500000000

partition.node2.param1=500000000
partition.node2.param2=999999999
```

2. Create the Tables (DDL)

This project requires that there be already existing databases with root access available to the administrator.

To get started, first adjust the *ddlfile* to contain the desired DDL:

```sql
DROP TABLE BOOKS;
CREATE TABLE BOOKS(isbn char(14),
				   title char(80),
				   price decimal);
```

Run the runDDL.py script.
The input to runDDL consists of two filenames (stored in variables clustercfg and ddlfile) passed in as command line arguments:

```bash
python cluster.py clustercfg ddlfile
```

The runDDL program will execute the same DDL on the database instance of each of the computers on the cluster concurrently using threads.
One thread is spawned for each connection in the cluster.
Here is an example of the output:

```bash
> python runDDL.py clustercfg ddlfile

================================================================================


                             PARSING clustercfg...
{
 "node1.passwd": "mypasswd",
 "node1.database": "josephyl2",
 "catalog.driver": "com.mysql.jdbc.Drive",
 "node1.username": "db2inst1",
 "catalog.passwd": "mypasswd",
 "catalog.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl1",
 "node2.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl3",
 "node1.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl2",
 "node2.passwd": "mypasswd",
 "node1.driver": "com.mysql.jdbc.Drive",
 "catalog.username": "db2inst1",
 "catalog.database": "josephyl1",
 "node2.database": "josephyl3",
 "node2.username": "db2inst1",
 "node2.driver": "com.mysql.jdbc.Drive",
 "catalog.numnodes": 2
}

--------------------------------------------------------------------------------

                            CREATING CONNECTIONS...

# of connections: 2

HOST: 127.0.0.1 DB: josephyl2 <pymysql.connections.Connection object at 0x10b5ed510>
HOST: 127.0.0.1 DB: josephyl3 <pymysql.connections.Connection object at 0x10b5ed690>

--------------------------------------------------------------------------------

                            PARSING SQL COMMANDS...

[SQL COMMANDS]:
CREATE TABLE BOOKS(isbn char(14), title char(80), author char(80))

TABLES:
['BOOKS']

--------------------------------------------------------------------------------

                              UPDATING CATALOG...

[SUCCESSFUL CATALOG CONNECTION] <127.0.0.1 - josephyl1> <pymysql.connections.Connection object at 0x10b5ed4d0>

DROP TABLE IF EXISTS dtables

CREATE TABLE dtables (tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));

INSERT INTO dtables VALUES ('BOOKS', 'com.mysql.jdbc.Drive', 'jdbc:mysql://127.0.0.1:3306/josephyl2', 'db2inst1','mypasswd', NULL,1,NULL,NULL,NULL);

INSERT INTO dtables VALUES ('BOOKS', 'com.mysql.jdbc.Drive', 'jdbc:mysql://127.0.0.1:3306/josephyl3', 'db2inst1','mypasswd', NULL,2,NULL,NULL,NULL);


--------------------------------------------------------------------------------

                       EXECUTING SQL COMMANDS ON NODES...

[JOB CREATED] <127.0.0.1 - josephyl2>
<pymysql.connections.Connection object at 0x10b5ed510>

[JOB CREATED] <127.0.0.1 - josephyl3>
<pymysql.connections.Connection object at 0x10b5ed690>

[JOB SUCCESSFUL] <127.0.0.1 - josephyl3>
[JOB SUCCESSFUL] <127.0.0.1 - josephyl2>

================================================================================
```

The metadata about the DDL being executed will be stored in a catalog database on the controller node.
The access information of the catalog database will be provided in the clustercfg file as well.
The metadata is stored in the following DDL:

```sql
dtables(tname     char(32),  -- is the table name involved in the DDL operation.
	   nodeid     int,       -- is the node number associated with this node.
	   nodedriver char(64),  --  is the driver used to connect to the node in the cluster for this entry
	   nodeurl    char(128), -- is the JDBC URL of the node in the cluster for this entry
	   nodeuser   char(16),  -- userid of the DBMS instance at the node in the cluster for this entry
	   nodepasswd char(16),  -- password of the DBMS instance at the node in the cluster for this entry
	   partmtd    int,       -- partition method used to partition the data in the table
	   partcol    char(32),  -- column(s) used by the partition method to partition the data in the table
	   partparam1 char(32),  -- parameters associated with the particular partition method
	   partparam2 char(32))  -- parameters associated with the particular partition method
```

If the table does not already exist in the catalog database, the program will create the table.
The field tname should be obtained using a simple parsing of the DDL for the keyword TABLE that precedes the table name.
This table is only updated on successful execution of the DDLs.
For create table DDL, this table is populated and for drop table DDLs, the relevant entries in this table should be deleted.
This operation is not multi-threaded.

3. Loading Data

Another script *loadCSV.py* is provided to assist in loading data into the database.
As mentioned previously, depending on the desired partition type, the data will be loaded into the cluster differently.
Supply the correct config file as the first command line argument and the comma separated value text file as the second argument.

An example csv file might look like:

```csv
324383414,"Coffee Explained","Li, Joe"
436363405,"I Need Help","Nakamura, Clay"
544923926,"Nightman Cometh","Kelly, Charlie"
653333487,"You Me and Beets","Schrute, Dwight"
764427428,"Could I be any funnier","Bing, Chandler"
875326409,"Sounds you love","Dong, Ding"
982325410,"Master of All","Lau, Gerald"
```

loadCSV.py output might look something like this:

```bash
> python loadCSV.py clustercfghash books.csv
                             READING CONFIG FILE...

Loading the CSV based hash partitioning

--------------------------------------------------------------------------------

                              UPDATING CATALOG...

THE NUMBER OF NODES IS 2
["UPDATE dtables SET partmtd = 2, partcol = 'isbn', partparam1 = 2, partparam2 = 'NULL' WHERE tname='books' AND nodeid = 1; ", "UPDATE dtables SET partmtd = 2, partcol = 'isbn', partparam1 = 2, partparam2 = 'NULL' WHERE tname='books' AND nodeid = 2; "]
[SUCCESSFUL CATALOG CONNECTION] <127.0.0.1 - josephyl1> <pymysql.connections.Connection object at 0x104e29810>


--------------------------------------------------------------------------------

                            CREATING CONNECTIONS...


HOST: 127.0.0.1 DB: josephyl2 <pymysql.connections.Connection object at 0x104e29690>
HOST: 127.0.0.1 DB: josephyl3 <pymysql.connections.Connection object at 0x104e29cd0>

--------------------------------------------------------------------------------

Using Hash Partitioning as Partitioning Method...

Index 0 of [u'isbn', u'title', u'author'] is the partition column

INSERT INTO BOOKS VALUES (123323232,'Database Systems','Ramakrishnan,Raghu') ;
INSERT INTO BOOKS VALUES (214323423,'Operating Systems','Silberstein, Adam') ;
INSERT INTO BOOKS VALUES (324383414,'Coffee Explained','Li, Joe') ;
INSERT INTO BOOKS VALUES (436363405,'I Need Help','Nakamura, Clay') ;
INSERT INTO BOOKS VALUES (544923926,'Nightman Cometh','Kelly, Charlie') ;
INSERT INTO BOOKS VALUES (653333487,'You Me and Beets','Schrute, Dwight') ;
INSERT INTO BOOKS VALUES (764427428,'Could I be any funnier','Bing, Chandler') ;
INSERT INTO BOOKS VALUES (875326409,'Sounds you love','Dong, Ding') ;
INSERT INTO BOOKS VALUES (982325410,'Master of All','Lau, Gerald') ;
```

4. Extracting data

At this point a user may want to extract data from a database.
You can use the runSQL.py script to run a select statement against the cluster.
An example sql statement might look like this:

```sql
SELECT *
FROM BOOKS;
```

And the resulting output might look something like this:

```bash
> python runSQL.py clustercfghash sqlfile

================================================================================


                           PARSING clustercfghash...
{
 "catalog.database": "josephyl1",
 "catalog.username": "db2inst1",
 "catalog.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl1",
 "catalog.driver": "com.mysql.jdbc.Drive",
 "catalog.passwd": "mypasswd"
}

--------------------------------------------------------------------------------

                               READING CATALOG...

select * from dtables where tname = 'BOOKS'
[SUCCESSFUL CATALOG CONNECTION] <127.0.0.1 - josephyl1> <pymysql.connections.Connection object at 0x103898fd0>


{
 "node1.tname": "BOOKS",
 "node2.tname": "BOOKS",
 "node2.username": "db2inst1",
 "node2.passwd": "mypasswd",
 "node1.passwd": "mypasswd",
 "node2.partparam2": "NULL",
 "node1.partparam2": "NULL",
 "node1.partmtd": 2,
 "node2.partparam1": "2",
 "catalog.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl1",
 "node1.database": "josephyl2",
 "catalog.driver": "com.mysql.jdbc.Drive",
 "node1.username": "db2inst1",
 "node1.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl2",
 "node1.driver": "com.mysql.jdbc.Drive",
 "catalog.username": "db2inst1",
 "catalog.database": "josephyl1",
 "node2.database": "josephyl3",
 "node2.driver": "com.mysql.jdbc.Drive",
 "node2.partmtd": 2,
 "node1.partparam1": "2",
 "catalog.passwd": "mypasswd",
 "node2.hostname": "jdbc:mysql://127.0.0.1:3306/josephyl3",
 "catalog.numnodes": 2
}

--------------------------------------------------------------------------------

                            CREATING CONNECTIONS...

# of connections: 2

HOST: 127.0.0.1 DB: josephyl2 <pymysql.connections.Connection object at 0x103898fd0>
HOST: 127.0.0.1 DB: josephyl3 <pymysql.connections.Connection object at 0x103898610>

--------------------------------------------------------------------------------

                       EXECUTING SQL COMMANDS ON NODES...

[JOB CREATED] <127.0.0.1 - josephyl2>
<pymysql.connections.Connection object at 0x1038c9d90>

[JOB CREATED] <127.0.0.1 - josephyl3>
<pymysql.connections.Connection object at 0x1038c9e50>

{u'author': u'Ramakrishnan,Raghu', u'isbn': u'123323232', u'title': u'Database Systems'}
{u'author': u'Li, Joe', u'isbn': u'324383414', u'title': u'Coffee Explained'}
 {u'author': u'Silberstein, Adam', u'isbn': u'214323423', u'title': u'Operating Systems'}
{u'author': u'Kelly, Charlie', u'isbn': u'544923926', {u'author': u'Nakamura, Clay', u'isbn': u'436363405', u'title': u'I Need Help'}
{u'author': u'Schrute, Dwight', u'isbn': u'653333487'u'title': u'Nightman Cometh'}
, u'title': u'You Me and Beets'}
{u'author': u'Bing, Chandler', u'isbn': u'764427428', u'title': u'Could I be any funnier'}
{u'author': u'Dong, Ding', u'isbn': u'875326409', u'title': u'Sounds you love'}
{u'author': u'Lau, Gerald', u'isbn': u'982325410', u'title': u'Master of All'}
[JOB SUCCESSFUL] <127.0.0.1 - josephyl3>
[JOB SUCCESSFUL] <127.0.0.1 - josephyl2>

================================================================================
```

## Contributions and Formatting

- Submissions to the code base are done via [Pull Requests](https://help.github.com/articles/about-pull-requests/)
- 4 space hard tabs are used in this project
