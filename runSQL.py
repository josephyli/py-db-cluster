import argparse
import csv
import os
import pymysql.cursors
import re
import sqlparse
import sys
import time
import threading

from ConfigParser import SafeConfigParser
from StringIO import StringIO
from collections import OrderedDict
from pymysql import OperationalError
from pymysql.cursors import DictCursorMixin, Cursor
from sqlparse import tokens
from sqlparse.sql import Identifier
from sqlparse.sql import IdentifierList
from sqlparse.tokens import DML
from sqlparse.tokens import Keyword

def test_for_table_name(configfilename):
	if os.path.isfile(configfilename):
		with open(configfilename) as stream:
			stream = StringIO("[fakesection]\n" + stream.read())

			# read/parse catalog data
			cp = SafeConfigParser()
			cp.readfp(stream)

			if cp.has_option('fakesection', 'tablename'):
				return True
			else:
				return False

def check_dtables_exists(config_dict):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	sql = "SELECT * FROM information_schema.tables WHERE table_schema = '%s' AND table_name = 'dtables' LIMIT 1;" % cat_db
	res = None;
	try:
		# connect and execute the sql statement
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)

		# print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
		print

		with connection.cursor() as cursor:
				res = cursor.execute(sql.strip() + ';')
				connection.commit()
	except pymysql.err.InternalError as d:
		print "[FAILED TO CHECK IF CATALOG EXISTS]"
		print d
	if res:
		return True
	else:
		return False


# stores metadata about the DDL in a catalog database
# using a list of tables that need to be created in the catalog
def update_DDL_catalog(config_dict, table_list):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	if check_dtables_exists(config_dict):
		sql = []
	else:
		sql = ["CREATE TABLE IF NOT EXISTS dtables (tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));"]

	# prepares the sql statement to insert into catalog the tables in each node
	for table in table_list:
		for i in range(config_dict["catalog.numnodes"]):
			hn = config_dict['node'+str(i + 1)+'.hostname']
			usr = config_dict['node'+str(i + 1)+'.username']
			pw = config_dict['node'+str(i + 1)+'.passwd']
			dr = config_dict['node'+str(i + 1)+'.driver']

			sql.append("INSERT INTO dtables VALUES (\'%s\', \'%s\', \'%s\', \'%s\',\'%s\', NULL,%d,NULL,NULL,NULL);" % (table,dr,hn,usr,pw,i+1))
	try:
		# connect and execute the sql statement
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)

		# print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
		print

		with connection.cursor() as cursor:
			# execute every sql command
			for command in sql:
				try:
					print command
					print
					cursor.execute(command.strip() + ';')
					connection.commit()
				except OperationalError, msg:
					print "Command skipped: ", msg
	except pymysql.err.InternalError as d:
		print "[FAILED TO UPDATE CATALOG]"
		print d

def getnumnodes(config_dict):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	sql = "SELECT MAX(nodeid) AS nodeid FROM dtables;"

	connection = pymysql.connect(host=cat_hn,
		user=cat_usr,
		password=cat_pw,
		db=cat_db,
		charset='utf8mb4',
		cursorclass=pymysql.cursors.DictCursor)
	with connection.cursor() as cursor:
		# execute every sql command
		try:
			cursor.execute(sql.strip() + ';')
			while True:
				row = cursor.fetchone()
				if row == None:
					break
				numnodes = row['nodeid']
		except OperationalError, msg:
			print "Error getting numnode: ", msg
			return -1
	return numnodes


def get_tables(sql_commands):
	from itertools import chain
	tables = []
	for command in sql_commands:
		tables.append(extract_tables(command))
	#extract the nested lists
	return list(chain.from_iterable(tables))

# returns a list of identifiers
def get_tables_real_names(sql_command):
	token_list = sqlparse.parse(sql_command)[0].tokens
	idenlist_index = 0
	for i in token_list:
		idenlist_index += 1
		if i.match(tokens.Keyword, 'from', False):
			break
	idenlist_index += 1
	iden_list = []
	for i in str(token_list[idenlist_index]).split(","):
		temp = i.lstrip()
		temp = temp.rstrip()
		t2 = temp.split(" ")[0]
		iden_list.append(t2)
	return iden_list

# ------- parsing table code-------------------------------------------------
def extract_table_identifiers(token_stream):
	for item in token_stream:
		if isinstance(item, IdentifierList):
			for identifier in item.get_identifiers():
				yield identifier.get_name()
		elif isinstance(item, Identifier):
			yield item.get_name()
		elif item.ttype is Keyword:
			yield item.value


def extract_tables(sql):
	stream = extract_from_part(sqlparse.parse(sql)[0])
	return list(extract_table_identifiers(stream))

def extract_from_part(parsed):
	from_seen = False
	for item in parsed.tokens:
		if from_seen:
			if is_subselect(item):
				for x in extract_from_part(item):
					yield x
			elif item.ttype is Keyword:
				raise StopIteration
			else:
				yield item
		elif item.ttype is Keyword and item.value.upper() == 'FROM':
			from_seen = True

def is_subselect(parsed):
	if not parsed.is_group:
		return False
	for item in parsed.tokens:
		if item.ttype is DML and item.value.upper() == 'SELECT':
			return True
	return False
# --------end of parsing table code--------------------------------------

# RUN SQL CODE ---------------------------------------------------------
# preserves column order
class OrderedDictCursor(DictCursorMixin, Cursor):
    dict_type = OrderedDict

# returns a list of sql commands as strings
def read_SQL(sqlfilename):
	f = open(sqlfilename, 'r')
	sqlfile = f.read()
	sqlfile.strip()
	f.close()
	temp = filter(None, sqlfile.split(';'))
	sql_commands = []
	# filter out white space from file input
	for c in temp:
		if c != "\n":
			sql_commands.append(c)
	return sql_commands

# returns a dict with all catalog information
# responsible for parsing the config file
def get_runSQL_config(configfilename):
	config_dict = {}

	if os.path.isfile(configfilename):
		with open(configfilename) as stream:
			# pass into string & add a header
			stream = StringIO("[fakesection]\n" + stream.read())

			# read/parse catalog data
			cp = SafeConfigParser()
			cp.readfp(stream)
			config_dict['catalog.driver'] = cp.get('fakesection', 'catalog.driver')
			config_dict['catalog.hostname'] = cp.get('fakesection', 'catalog.hostname')
			config_dict['catalog.username'] = cp.get('fakesection', 'catalog.username')
			config_dict['catalog.passwd'] = cp.get('fakesection', 'catalog.passwd')
			config_dict['catalog.database'] = cp.get('fakesection', 'catalog.hostname').rsplit('/', 1)[-1]

			if cp.has_option('fakesection', 'localnode.driver'):
				config_dict['localnode.driver'] = cp.get('fakesection', 'localnode.driver')
				config_dict['localnode.hostname'] = cp.get('fakesection', 'localnode.hostname')
				config_dict['localnode.username'] = cp.get('fakesection', 'localnode.username')
				config_dict['localnode.passwd'] = cp.get('fakesection', 'localnode.passwd')
				config_dict['localnode.database'] = cp.get('fakesection', 'localnode.hostname').rsplit('/', 1)[-1]

			# read the number of nodes
			if cp.has_option('fakesection', 'numnodes'):
				numnodes = cp.getint('fakesection', 'numnodes')
				config_dict['catalog.numnodes'] = numnodes

			# read node data and print out info
			if cp.has_option('fakesection', 'node1.driver'):
				for node in range(1, numnodes + 1):
					for candidate in ['driver', 'hostname', 'username', 'passwd', 'database']:
						# test if candidate exists before adding to dictionary
						if cp.has_option('fakesection', "node" + str(node) + "." + candidate):
							# print cp.get('fakesection', "node" + str(node) + "." + candidate)
							config_dict["node" + str(node) + "." + candidate] = cp.get('fakesection', "node" + str(node) + "." + candidate)
						else:
							if candidate == "database":
								config_dict["node" + str(node) + ".database"] = cp.get('fakesection', "node" + str(node) + ".hostname").rsplit('/', 1)[-1]
							else:
								print "error: candidate not found"

			return config_dict
	else:
		print("No config file found at", configfilename)
		return null

def loadCSV(configfilename, csvfilename):
	csv_list = []
	with open(csvfilename, 'rU') as csvfile:
		dialect = csv.Sniffer().sniff(csvfile.read(), delimiters=',|;')
		csvfile.seek(0)
		reader = csv.reader(csvfile, dialect)

		try:
			for row in reader:
				csv_list.append(row)
		except:
			print row

	return csv_list

# returns a dict with all nodes information
# responsible for parsing the config file
def get_loadcsv_config(configfilename):
	config_dict = {}

	if os.path.isfile(configfilename):
		with open(configfilename) as stream:
			# pass into string & add a header
			stream = StringIO("[fakesection]\n" + stream.read())

			# read/parse catalog data
			cp = SafeConfigParser()
			cp.readfp(stream)
			config_dict['catalog.driver'] = cp.get('fakesection', 'catalog.driver')
			config_dict['catalog.hostname'] = cp.get('fakesection', 'catalog.hostname')
			config_dict['catalog.username'] = cp.get('fakesection', 'catalog.username')
			config_dict['catalog.passwd'] = cp.get('fakesection', 'catalog.passwd')
			config_dict['catalog.database'] = cp.get('fakesection', 'catalog.hostname').rsplit('/', 1)[-1]

			config_dict['catalog.tablename'] = cp.get('fakesection', 'tablename')

			partition_method_string = cp.get('fakesection', 'partition.method')
			if partition_method_string=='notpartition':
				config_dict['catalog.partition.method'] = 0
			if partition_method_string=='range':
				config_dict['catalog.partition.method'] = 1
			elif partition_method_string == 'hash':
				config_dict['catalog.partition.method'] = 2

			if cp.has_option('fakesection', 'partition.column'):
				config_dict['catalog.partition.column'] = cp.get('fakesection', 'partition.column')

			numnodes = 0
			# read the number of nodes... if it's listed
			if cp.has_option('fakesection', 'numnodes'):
				numnodes = cp.getint('fakesection', 'numnodes')
				config_dict['catalog.numnodes'] = numnodes
			else:
				# connect to catalog to get the supposed number of nodes
				config_dict["catalog.numnodes"] = getnumnodes(config_dict)
				numnodes = config_dict["catalog.numnodes"]

			# read depending on partition method
			if (partition_method_string == 'notpartition'):
				print "Loading the CSV based not-partitioning"
				if numnodes != config_dict['catalog.numnodes']:
					print "Error! dtables's number of nodes does not match the partitioning. Exiting..."
					sys.exit(1)
				return config_dict

			elif (partition_method_string == 'range'):
				print "Loading the CSV based range partitioning"
				config_dict['partition.column'] = cp.get('fakesection', 'partition.column')
				# read node data and compare to dtables
				node = 1
				while (cp.has_option('fakesection', "partition.node" + str(node) + ".param1")) or (cp.has_option('fakesection', "partition.node" + str(node) + ".param2")):
					for parameter in ['param1', 'param2']:
						# test if candidate exists before adding to dictionary
						if cp.has_option('fakesection', "partition.node" + str(node) + "." + parameter):
							config_dict["partition.node" + str(node) + "." + parameter] = cp.getint('fakesection', "partition.node" + str(node) + "." + parameter)
					node += 1
				# check if dtables matches number in partition
				if numnodes != node - 1:
					print "Error! dtables's number of nodes does not match the partitioning. Exiting..."
					sys.exit(1)
				return config_dict

			elif (cp.get('fakesection', 'partition.method') == 'hash'):
				print "Loading the CSV based hash partitioning"
				config_dict['catalog.numnodes'] = cp.getint('fakesection', 'partition.param1')
				config_dict['partition.column'] = cp.get('fakesection', 'partition.column')
				config_dict['partition.param1'] = cp.getint('fakesection', 'partition.param1')
				numnodes = config_dict['partition.param1']
				config_dict['catalog.numnodes'] = numnodes
				if numnodes != config_dict['partition.param1']:
					print "Error! dtables's number of nodes does not match the partitioning. Exiting..."
					sys.exit(1)
				return config_dict
	else:
		print("No config file found at", configfilename)
		return null

# update node data from catalog
def update_catalog_with_partitions(config_dict):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	tablename = config_dict['catalog.tablename']
	pm = config_dict['catalog.partition.method']

	if pm != 0:
		pc = config_dict['catalog.partition.column']

	try:
		number_of_nodes = config_dict["catalog.numnodes"]
	except KeyError as e:
		print "The number of nodes was not specified in the config file..."

	print "THE NUMBER OF NODES IS",  number_of_nodes

	sql = []

	# if using a hashing partition method, the table is updated differently
	if pm == 2:
		p1 = config_dict['partition.param1']
		for i in range(number_of_nodes):
			sql.append("UPDATE dtables SET partmtd = 2, partcol = \'%s\', partparam1 = %d, partparam2 = \'NULL\' WHERE tname=\'%s\' AND nodeid = %d; " % (pc, int(p1), tablename, int(i) + 1))
		print sql
	elif pm == 1:
		# prepares the sql statement to insert into catalog the tables in each node for range
		for nodeid in range(1, number_of_nodes + 1):
			try:
				p1 = config_dict['partition.node'+str(nodeid)+'.param1']
				p2 = config_dict['partition.node'+str(nodeid)+'.param2']
			except KeyError:
				print "Partitions in configuration file do not match the number of nodes in catalog... Exiting"
				sys.exit(1)
			sql.append("UPDATE dtables SET partmtd = %d, partcol = \'%s\', partparam1 = %d, partparam2 = %d WHERE tname=\'%s\' AND nodeid = %d; " % (pm, pc, p1, p2, tablename, nodeid))
	elif pm == 0:
		for i in range(number_of_nodes):
			sql.append("UPDATE dtables SET partmtd = %d, partcol = NULL, partparam1 = NULL, partparam2 = NULL WHERE tname=\'%s\' AND nodeid = %d; " % (int(pm), tablename, int(i) + 1))
	try:
		# connect and execute the sql statement
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)

		# print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
		print

		with connection.cursor() as cursor:
			# execute every sql command
			for command in sql:
				try:
					cursor.execute(command.strip() + ';')
					connection.commit()
				except OperationalError, msg:
					print "Command skipped: ", msg
	except pymysql.err.InternalError as d:
		print "[FAILED TO UPDATE CATALOG]"
		print d


	# read the node data
	sql = ["select * from dtables;"]
	node_list = []
	try:
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)
		with connection.cursor() as cursor:
			# execute every sql command
			for command in sql:
				try:
					cursor.execute(command.strip() + ';')
					while True:
						row = cursor.fetchone()
						if row == None:
							break
						node_list.append(row)
				except OperationalError, msg:
					print "Command skipped: ", msg
					connection.commit()

	except:
			print "couldn't connect to catalog"
	if node_list:
		# access the list of node dicts
		for entry in node_list:
			nodeid = entry["nodeid"]

			config_dict['node'+str(nodeid)+'.hostname'] = entry['nodeurl']
			config_dict['node'+str(nodeid)+'.partmtd'] = entry['partmtd']
			config_dict['node'+str(nodeid)+'.partparam1'] = entry['partparam1']
			config_dict['node'+str(nodeid)+'.partparam2'] = entry['partparam2']
			config_dict['node'+str(nodeid)+'.driver'] = entry['nodedriver']
			config_dict['node'+str(nodeid)+'.username'] = entry['nodeuser']
			config_dict['node'+str(nodeid)+'.tname'] = entry['tname']
			config_dict['node'+str(nodeid)+'.passwd'] = entry['nodepasswd']
			config_dict['node'+str(nodeid)+'.database'] = entry['nodeurl'].rsplit('/', 1)[-1]
	else:
		config_dict['catalog.numnodes'] = 0
	return config_dict

def not_partitioned_insert(csv_list, node_connections, config_dict):
	print
	numnodes = config_dict['catalog.numnodes']

	# construct the sql_statement
	values = ', '.join(["%s" for i in range(len(csv_list[0]))])
	sql_statement = "INSERT INTO " + config_dict['catalog.tablename'].upper() + " VALUES ({a})".format(a=values)
	args = ()
	try:
		for i in range(len(csv_list)):
			args = tuple(csv_list[i])
			for node_index in range(1, numnodes+1):
				with node_connections[node_index-1].cursor() as cursor:
					cursor.execute(sql_statement, args)
					res = cursor.fetchone()
					node_connections[node_index-1].commit()
					print "data committed to node " + str(node_index)
	except pymysql.MySQLError as e:
		print e
	finally:
		cursor.close()

def range_insert(csv_list, node_connections, config_dict):
	res = ""
	columns = []

	# identifying the partition column
	for connection in node_connections:
		with connection.cursor() as cursor:
			try:
				cursor.execute("SHOW COLUMNS IN " +config_dict['catalog.tablename'].upper() + ";")
				res = cursor.fetchall()
				connection.commit()
			except pymysql.MySQLError as e:
				print e
	for d in res:
		columns.append(d['Field'])

	partition_index = 0
	for f in columns:
		if f.lower() == config_dict['partition.column'].lower():
			break
		partition_index += 1

	# print "Index",partition_index,"of",columns,"is the partition column"
	numnodes = config_dict['catalog.numnodes']

	try:
		# construct the sql_statement
		values = ', '.join(["%s" for i in columns])
		sql_statement = "INSERT INTO " + config_dict['catalog.tablename'].upper() + " VALUES ({a})".format(a=values)
		args = ()

		for i in range(len(csv_list)):
			args = tuple(csv_list[i])
			for node_index in range(1, numnodes+1):
				# if the item in csv is greater than the lower limit (param1) but smaller than the upper limit (param2) of the node_index
				if ((int(csv_list[i][partition_index]) > int(config_dict['partition.node'+str(node_index)+'.param1'])) and (int(csv_list[i][partition_index]) <= int(config_dict['partition.node'+str(node_index)+'.param2']))):
					with node_connections[node_index-1].cursor() as cursor:
						cursor.execute(sql_statement,args)
						res = cursor.fetchone()
						node_connections[node_index-1].commit()
						print "data committed to node " + str(node_index)
	except pymysql.MySQLError as e:
		print e
	finally:
		cursor.close()



def hash_insert(csv_list, node_connections, config_dict):
	res = ""
	columns = []

	# identifying the partition column
	for connection in node_connections:
		with connection.cursor() as cursor:
			try:
				cursor.execute("SHOW COLUMNS IN " +config_dict['catalog.tablename'].upper() + ";")
				res = cursor.fetchall()
				connection.commit()
			except pymysql.MySQLError as e:
				print e
	for d in res:
		columns.append(d['Field'])

	partition_index = 0
	for f in columns:
		if f.lower() == config_dict['partition.column'].lower():
			break
		partition_index += 1

	# print "Index",partition_index,"of",columns,"is the partition column"

	# construct the sql_statement
	values = ', '.join(["%s" for i in columns])
	sql_statement = "INSERT INTO " + config_dict['catalog.tablename'].upper() + " VALUES ({a})".format(a=values)
	args = ()
	good_count = 0
	bad_count = 0
	for i in range(len(csv_list)):
		# convert each row into a tuple to be passed
		try:
			args = tuple(csv_list[i])

			nodeid = int(csv_list[i][partition_index]) % int(config_dict['catalog.numnodes'])
			with node_connections[nodeid].cursor() as cursor:
				cursor.execute(sql_statement, args)
				res = cursor.fetchone()
				node_connections[nodeid].commit()
				good_count += 1
		except pymysql.MySQLError as e:
			print e
			bad_count += 1
	print "{0} rows to {1} were commited".format(good_count, config_dict['catalog.tablename'])
	print "{0} rows were not committed".format(bad_count)
	cursor.close()

# reads metadata about the nodes from the catalog database
# uses a list of tables that need to be created in the catalog to know what nodes are needed
def read_catalog(config_dict, table_list):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	# make the sql to select all nodes from the tables list
	sql = "select * from dtables where tname = \'" + table_list[0] + "\'"
	itercars = iter(table_list)
	next(itercars)
	for car in itercars:
		sql = sql + " OR tname = \'" + car + '\''
	# print sql

	# read the node data
	node_list = []
	try:
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)
		# print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
		# print

		with connection.cursor() as cursor:
			# select every node with the table name from the sqlfile
			try:
				cursor.execute(sql.strip() + ';')
				while True:
					row = cursor.fetchone()
					if row == None:
						# print
						break
					node_list.append(row)
			except OperationalError, msg:
				print "Command skipped: ", msg
				connection.commit()

	except:
			print "read_catalog: couldn't connect to catalog"

	# if node list is not empty, then pass it into the config_dict
	if node_list:
		config_dict['catalog.numnodes'] = getnumnodes(config_dict)
		# access the list of node dicts
		for entry in node_list:
			nodeid = entry["nodeid"]

			config_dict['node'+str(nodeid)+'.hostname'] = entry['nodeurl']
			config_dict['node'+str(nodeid)+'.partmtd'] = entry['partmtd']
			config_dict['node'+str(nodeid)+'.partparam1'] = entry['partparam1']
			config_dict['node'+str(nodeid)+'.partparam2'] = entry['partparam2']
			config_dict['node'+str(nodeid)+'.driver'] = entry['nodedriver']
			config_dict['node'+str(nodeid)+'.username'] = entry['nodeuser']
			config_dict['node'+str(nodeid)+'.tname'] = entry['tname']
			config_dict['node'+str(nodeid)+'.passwd'] = entry['nodepasswd']
			config_dict['node'+str(nodeid)+'.database'] = entry['nodeurl'].rsplit('/', 1)[-1]
	else:
		print "read_catalog: No tables information found on dtables"
		config_dict['catalog.numnodes'] = 0
		update_DDL_catalog(config_dict,table_list)
	return config_dict


# returns a list of connections to all nodes
def get_connections(config_dict):
	connections = []

	for i in range(config_dict["catalog.numnodes"]):
		try:
			hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['node'+str(i + 1)+'.hostname'] )[0]
			usr = config_dict['node'+str(i + 1)+'.username']
			pw = config_dict['node'+str(i + 1)+'.passwd']
			db = config_dict['node'+str(i + 1)+'.database']
			connections.append(pymysql.connect(host=hn,
											user=usr,
											password=pw,
											db=db,
											charset='utf8mb4',
											cursorclass=pymysql.cursors.DictCursor))
		except:
			print "[NODE", i +  1, "CONNECTION FAILED]:"
			print "hostname:".rjust(12), re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['node'+str(i + 1)+'.hostname'] )[0]
			print "username:".rjust(12), config_dict['node'+str(i + 1)+'.username']
			print "password:".rjust(12), config_dict['node'+str(i + 1)+'.passwd']
			print "database:".rjust(12), config_dict['node'+str(i + 1)+'.database']
			print
	return connections

# runs the list of commands against the list of connections
# later, this will implement multi-threading
def run_commmands_against_nodes(connections, sql_commands):
	import time
	from threading import Thread
	from threading import active_count

	# create a list of jobs
	list_of_threads = []
	for connection in connections:
			print "[JOB CREATED] <"+ connection.host+ " - " + connection.db+ ">"
			print connection
			list_of_threads.append(Thread(target=run_sql_commands_against_node, args=(connection, sql_commands)))
			print
	# start up all jobs
	for t in list_of_threads:
		t.start()
	# wait for all jobs to complete before moving on
	while active_count() > 1:
		time.sleep(1)


def run_sql_commands_against_node(connection, sql_commands):
	with connection.cursor() as cursor:
		try:
			for c in sql_commands:
				cursor.execute(c.strip() + ';')
				# while True:
				d = cursor.fetchall()
				if d == None:
					break
				printTable(d)

			connection.commit()
			print
			print "[JOB SUCCESSFUL] <"+connection.host+ " - " + connection.db+ ">"
			connection.close()
		except pymysql.MySQLError as e:
			print "[JOB FAILED] <"+connection.host+ " - " + connection.db+ "> ERROR: {!r}, ERROR NUMBER: {}".format(e, e.args[0])

def detect_join(sql_statement):
	# ensure that the input is a string
	if not isinstance(sql_statement, basestring):
		if args.verbose:
			sys.stderr.write(sql_statement + " is not a string")
		return False

	# temporarily make it lower case for easier comparisons
	sql_statement = sql_statement.lower()

	# ensures that there is only one sql statement given statement
	# returns false if there is not only one sql statement passed in
	num_statements = len(sqlparse.parse(sql_statement))
	if num_statements != 1:
		if args.verbose:
			sys.stderr.write("'" + str(sql_statement) + "' contains " + num_statements + " statements when it should only have one")
		return False

	if " join " in sql_statement:
		return True

	try:
		if len(get_tables_real_names(sql_statement)) > 1:
			return True
		else:
			return False
	except Exception:
		return False


def join_tables(config_dict, connections, input_sql_query):
	#This function uses the config_dict, existing connections, and list of tables to join tables together
	# identify the localnode which will have the temporary table and create the temp table
	l_hn = config_dict['localnode.hostname']
	l_db = config_dict['localnode.database']

	table_list = get_tables_real_names(input_sql_query)
	table1 = table_list[0]
	table2 = table_list[1]

	for nodeid in range(1, config_dict["catalog.numnodes"]+1):
		if (config_dict['node'+str(nodeid)+'.database']==l_db) and (config_dict['node'+str(nodeid)+'.hostname'] == l_hn):
			localnodeid = nodeid
			# print "The localnode is node " + str(localnodeid) + " that will coordinate work with other nodes..."
			try:
				localnodecursor = connections[localnodeid-1].cursor(OrderedDictCursor)
				create_temp_sql = "CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS (SELECT * FROM {1})".format(table1.upper(), table1.upper())
				# print create_temp_sql
				localnodecursor.execute(create_temp_sql.strip() + ';')
				connections[localnodeid-1].commit()
				create_temp_sql_2 = "CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS (SELECT * FROM {1})".format(table2.upper(), table2.upper())
				# print create_temp_sql_2
				localnodecursor.execute(create_temp_sql_2.strip() + ';')
				connections[localnodeid-1].commit()
			except pymysql.MySQLError as e:
				print e
			finally:
				break

	# if using hash or range partition, then read from other nodes using threads...
	if config_dict['node1.partmtd'] != 0:
		t = threading.Thread(target=move_table, args = (connections, localnodeid, table1, OrderedDictCursor, localnodecursor))
		t.start()

		while threading.active_count() > 1:
			time.sleep(1)

		f = threading.Thread(target=move_table, args = (connections, localnodeid, table2, OrderedDictCursor, localnodecursor))
		f.start()

		while threading.active_count() > 1:
			time.sleep(1)

	# use the original query on the new temporary tables
	try:
		localnodecursor.execute(input_sql_query.strip() + ';')
		d = localnodecursor.fetchall()
		printTable(d)
	except pymysql.MySQLError as e:
		print e
	finally:
		localnodecursor.close()

def move_table(connections, localnodeid, input_table, OrderedDictCursor, localnodecursor):
	for count,connection in enumerate(connections):
		if count + 1 != localnodeid:
			select_sql = "SELECT * FROM {0}".format(input_table)

			try:
				cursor = connection.cursor(OrderedDictCursor)
				cursor.execute(select_sql.strip() + ';')
				# while True:
				results = cursor.fetchall()
				if results == None:
					break
				connection.commit()
				for row in results:
					rowargs = tuple(row.values())
					# construct the sql_statement
					values = ', '.join(["%s" for i in range(len(row))])
					sql_statement = "INSERT INTO {0} VALUES ({a})".format(input_table, a=values)
					try:
						# print rowargs
						localnodecursor.execute(sql_statement, rowargs)
						res = localnodecursor.fetchone()
						connections[localnodeid-1].commit()
					except pymysql.MySQLError as e:
						print e

			except pymysql.MySQLError as e:
				print "[JOB FAILED] <"+connection.host+ " - " + connection.db+ "> ERROR: {!r}".format(e)


def select_table(config_dict, connections, input_sql_query):
	#This function uses the config_dict, existing connections, and list of tables to join tables together
	# identify the localnode which will have the temporary table and create the temp table
	l_hn = config_dict['localnode.hostname']
	l_db = config_dict['localnode.database']

	table_list = get_tables_real_names(input_sql_query)
	table1 = table_list[0]

	for nodeid in range(1, config_dict["catalog.numnodes"]+1):
		if (config_dict['node'+str(nodeid)+'.database']==l_db) and (config_dict['node'+str(nodeid)+'.hostname'] == l_hn):
			localnodeid = nodeid
			# print "The localnode is node " + str(localnodeid) + " that will coordinate work with other nodes..."
			try:
				localnodecursor = connections[localnodeid-1].cursor(OrderedDictCursor)
				create_temp_sql = "CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS (SELECT * FROM {1})".format(table1.upper(), table1.upper())
				# print create_temp_sql
				localnodecursor.execute(create_temp_sql.strip() + ';')
				connections[localnodeid-1].commit()
			except pymysql.MySQLError as e:
				print e
			finally:
				break

	# if using hash or range partition, then read from other nodes using threads...
	if config_dict['node1.partmtd'] != 0:
		t = threading.Thread(target=move_table, args = (connections, localnodeid, table1, OrderedDictCursor, localnodecursor))
		t.start()

		while threading.active_count() > 1:
			time.sleep(1)

	# use the original query on the new temporary table
	try:
		localnodecursor.execute(input_sql_query.strip() + ';')
		d = localnodecursor.fetchall()
		printTable(d)
	except pymysql.MySQLError as e:
		print e
	finally:
		localnodecursor.close()

# somewhat based on http://stackoverflow.com/questions/17330139/python-printing-a-dictionary-as-a-horizontal-table-with-headers
def printTable(myDict, colList=None):
	some_lock = threading.Lock()
	with some_lock:
		if not colList:
			colList = list(myDict[0].keys() if myDict else [])
		myList = []
		for item in myDict:
			myList.append([str(item[col] or '') for col in colList])
		colSize = [max(map(len,col)) for col in zip(*myList)]
		formatStr = ' '.join(["{{:<{}}}".format(i+5) for i in colSize])
		# myList.insert(1, ['-' * i for i in colSize]) # Seperating line
		for item in myList: print(formatStr.format(*item))

def print_pretty_dict(idict):
	import json
	print json.dumps(idict, indent=1)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("csvfile_or_sqlfile", help="Location of CSV or SQL File, See the README for more information")
	global args
	args = parser.parse_args()

	if test_for_table_name(args.configfile):
		# table detected -> run load CSV
		if args.csvfile_or_sqlfile[-4:].lower() != ".csv":
			print "Are you certain you are providing a CSV file?"
			sys.exit(1)
		# read the config file into a dictionary
		print "READING CONFIG FILE...".center(80, " ")
		print
		config_dict = get_loadcsv_config(args.configfile)
		print
		print "-" * 80
		print

		# update the catalog
		print "UPDATING CATALOG...".center(80, " ")
		print
		node_list = update_catalog_with_partitions(config_dict)
		print
		print "-" * 80
		print

		# return a list of connections to all nodes
		print "CREATING CONNECTIONS...".center(80, " ")
		print
		node_connections = get_connections(config_dict)
		if len(node_connections) == 0:
			print "Terminating due to connection failures..."
			sys.exit()
		print
		# for c in node_connections:
			# print "HOST: " + c.host + " DB: " + c.db + " " + str(c)
		# print
		print "-" * 80
		print

		# read the csv file into a list
		csv_list = loadCSV(args.configfile, args.csvfile_or_sqlfile)

		# handle different types of partitioning methods
		if config_dict['catalog.partition.method'] == 0: # if no partitioning scheme is set - csv is propagated to all nodes
			print "Using Not-Partitioned Partitioning as Partitioning Method..."
			not_partitioned_insert(csv_list, node_connections, config_dict)


		elif config_dict['catalog.partition.method'] == 1: # if range partitioning is set
			print "Using Range Partitioning as Partitioning Method..."
			range_insert(csv_list, node_connections, config_dict)

		elif config_dict['catalog.partition.method'] == 2: # if hash partitioning is set
			print "Using Hash Partitioning as Partitioning Method..."
			hash_insert(csv_list, node_connections, config_dict)

	else:
		# run SQL
		# read configuration and return a dictionary -------------------------------
		temp = "PARSING " + str(args.configfile) + "..."
		# print
		# print temp.center(80, " ")
		config_dict = get_runSQL_config(args.configfile)

		# print_pretty_dict(config_dict)

		# read sql commands for a list of tables -----------------------------------
		sql_commands = read_SQL(args.csvfile_or_sqlfile)
		table_list = []

		if sql_commands[0].split()[0].upper() == "CREATE":
			# print "CREATE TABLE DETECTED IN DDL... UPDATING CATALOG WITH NEW TABLES...".center(80, " ")
			for command in sql_commands:
				if command.split()[0].upper() == "CREATE":
					table_list.append((re.split('\s|\(',command)[2]))
			node_connections = get_connections(config_dict)
			# print table_list
			update_DDL_catalog(config_dict,table_list)
			run_commmands_against_nodes(node_connections, sql_commands)
		else:
			table_list = get_tables_real_names(sql_commands[0])
			# print
			# print "-" * 80
			# print

			# read catalog for a list of node -----------------------------------
			# print "READING CATALOG...".center(80, " ")
			# print
			nodes_dict = read_catalog(config_dict, table_list)
			# print_pretty_dict(nodes_dict)
			# print
			# print "-" * 80
			# print

			# run the commands against the nodes ---------------------------------------
			# print "EXECUTING SQL COMMANDS ON NODES...".center(80, " ")
			# print
			node_connections = get_connections(nodes_dict)
			if len(node_connections) == 0:
				print "Terminating due to connection failures..."
				sys.exit()

			# If a join is deteched, then run join table method
			if detect_join(sql_commands[0]):
				for command in sql_commands:
					# print "Results: "
					join_tables(nodes_dict, node_connections, command)
					# print
			else:
				# no join, no worries
				for command in sql_commands:
					# print "Results: "
					select_table(nodes_dict, node_connections, command)
					# print


if __name__ == "__main__":
	main()
