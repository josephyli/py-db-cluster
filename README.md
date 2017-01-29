# py-db-cluster
Python Scripts to Control Database Cluster

*PROJECT IN ALPHA*

## To Do List

- add installation instructions to the *Installation* section
- Seperate cluster.py into OOP methods
- use arg parser to take in arguments
- documentation should have an example picture to demonstrate usage of project
- minimally support the drop table and create table DDLs.
- include instructions for all pre-requisite steps to get the program running
- describe all input configuration files, parameters etc.
- describe expected output and error conditions
- write documentation to Google docs
- be at least 3 pages long in size 12 fonts not including n diagrams.

## Overview

The parallel SQL processing system consists of a cluster of DBMS instances running on different machines.
DDLs are translated into corresponding DDLs for each individual DBMS instance in the cluster and executed there.
In addition, a catalog database on the controller node stores metadata about what data is stored for each table on each DBMS instance in the cluster.

## Installation

** WRITE ME **

## Usage

Adjust the *clustercfg* to contain access information for each computer on the cluster like so:

```
catalog.driver=com.ibm.db2.jcc.DB2Driver
catalog.hostname=jdbc:db2://10.0.0.3:50001/mycatdb
catalog.username=db2inst1
catalog.passwd=mypasswd

numnodes=2

node1.driver=com.ibm.db2.jcc.DB2Driver
node1.hostname=jdbc:db2://10.0.0.3:50001/mydb1
node1.username=db2inst1
node1.passwd=mypasswd

node2.driver=com.ibm.db2.jcc.DB2Driver
node2.hostname=jdbc:db2://10.0.0.3:50001/mydb2
node2.username=db2inst1
node2.passwd=mypasswd
```

Adjust the *ddlfile* to contain the DDL terminated by a semi-colon to be executed:

```sql
CREATE TABLE BOOKS(isbn char(14), title char(80), price
decimal);
```

The input to runDDL consists of two filenames (stored in variables clustercfg and ddlfile) passed in as command line arguments:

```bash
python cluster.py clustercfg ddlfile
```

The runDDL program will execute the same DDL on the database instance of each of the computers on the cluster concurrently using threads.
One thread is spawned for each computer in the cluster.
The runDDL program will report success or failure of executing the DDL for each node in the cluster to standard output.

The metadata about the DDL being executed will be stored in a catalog database on the controller node. The access information of the catalog database will be provided in the clustercfg file as well. The metadata is stored in the following DDL:

```sql
dtables(tname char(32), -- is the table name involved in the DDL operation.
   nodeid int,-- is the node number associated with this node.
   nodedriver char(64), --  is the driver used to connect to the node in the cluster for this entry
   nodeurl char(128), -- is the JDBC URL of the node in the cluster for this entry
   nodeuser char(16), -- userid of the DBMS instance at the node in the cluster for this entry
   nodepasswd char(16), -- password of the DBMS instance at the node in the cluster for this entry
   partmtd int, -- partition method used to partition the data in the table
   partcol char(32), -- column(s) used by the partition method to partition the data in the table
   partparam1 char(32), -- parameters associated with the particular partition method
   partparam2 char(32)) -- parameters associated with the particular partition method
```

If the table does not already exist in the catalog database, the program will create the table.
The field tname should be obtained using a simple parsing of the DDL for the keyword TABLE that precedes the table name.
This table is only updated on successful execution of the DDLs.
For create table DDL, this table is populated and for drop table DDLs, the relevant entries in this table should be deleted.
This operation is not multi-threaded.

Sample Output:

```bash
./run.sh ./cluster.cfg ./books.sql
[jdbc:db2://10.0.0.3:50001/mydb2]: ./books.sql success.
[jdbc:db2://10.0.0.3:50001/mydb1]: ./books.sql failed.
[jdbc:db2://10.0.0.3:50001/mycatdb]: catalog updated.
```
