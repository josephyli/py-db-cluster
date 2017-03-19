import argparse
import os
import pymysql.cursors
import re
import sqlparse
import sys
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

# returns a dict with all catalog information
# responsible for parsing the config file
def get_config(configfilename):
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

			config_dict['localnode.driver'] = cp.get('fakesection', 'localnode.driver')
			config_dict['localnode.hostname'] = cp.get('fakesection', 'localnode.hostname')
			config_dict['localnode.username'] = cp.get('fakesection', 'localnode.username')
			config_dict['localnode.passwd'] = cp.get('fakesection', 'localnode.passwd')
			config_dict['localnode.database'] = cp.get('fakesection', 'localnode.hostname').rsplit('/', 1)[-1]

			return config_dict
	else:
		print("No config file found at", configfilename)
		return null

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
	print sql

	# read the node data
	node_list = []
	try:
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)
		print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
		print

		with connection.cursor() as cursor:
			# select every node with the table name from the sqlfile
			try:
				cursor.execute(sql.strip() + ';')
				while True:
					row = cursor.fetchone()
					if row == None:
						print
						break
					node_list.append(row)
			except OperationalError, msg:
				print "Command skipped: ", msg
				connection.commit()

	except:
			print "couldn't connect to catalog"

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
		config_dict['catalog.numnodes'] = 0
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


def join_tables(config_dict, connections, table1, table2, input_sql_query):
	#This function uses the config_dict, existing connections, and list of tables to join tables together
	#connect to dtables, then get results from each node
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	# form statement to select all nodes with table1
	sql = "SELECT * FROM dtables WHERE tname = \'" + table1 + "\'"

	# connect to catalog to get table1's partitions
	part_list1 = []
	try:
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)
		with connection.cursor() as cursor:
			# select every node with the table name from the sqlfile
			try:
				cursor.execute(sql.strip() + ';')
				while True:
					row = cursor.fetchone()
					if row == None:
						print
						break
					node_list1.append(row)
			except OperationalError, msg:
				print "Command skipped: ", msg
				connection.commit()
	except:
			print "couldn't connect to catalog"

	# form statement to select all partitions with table1
	sql = "SELECT * FROM dtables WHERE tname = \'" + table2 + "\'"

	# connect to catalog to get table2's partitions
	part_list2 = []
	try:
		with connection.cursor() as cursor:
			# select every node with the table name from the sqlfile
			try:
				cursor.execute(sql.strip() + ';')
				while True:
					row = cursor.fetchone()
					if row == None:
						print
						break
					part_list2.append(row)
			except OperationalError, msg:
				print "Command skipped: ", msg
				connection.commit()
	except:
			print "couldn't connect to catalog"
	finally:
		connection.close()

	# identify the localnode which will have the temporary table and create the temp table
	l_hn = config_dict['localnode.hostname']
	l_db = config_dict['localnode.database']
	for nodeid in range(1, config_dict["catalog.numnodes"]+1):
		if (config_dict['node'+str(nodeid)+'.database']==l_db) and (config_dict['node'+str(nodeid)+'.hostname'] == l_hn):
			localnodeid = nodeid
			print "The localnode is node " + str(localnodeid) + " that will coordinate work with other nodes..."
			try:
				localnodecursor = connections[localnodeid-1].cursor(OrderedDictCursor)
				create_temp_sql = "CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS (SELECT * FROM {1})".format(table1.upper(), table1.upper())
				print create_temp_sql
				localnodecursor.execute(create_temp_sql.strip() + ';')
				connections[localnodeid-1].commit()
				create_temp_sql_2 = "CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS (SELECT * FROM {1})".format(table2.upper(), table2.upper())
				print create_temp_sql_2
				localnodecursor.execute(create_temp_sql_2.strip() + ';')
				connections[localnodeid-1].commit()
			except pymysql.MySQLError as e:
				print e
			finally:
				break

	move_table(connections, localnodeid, table1, OrderedDictCursor, localnodecursor)
	move_table(connections, localnodeid, table2, OrderedDictCursor, localnodecursor)

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
					finally:
						cursor.close()
			except pymysql.MySQLError as e:
				print "[JOB FAILED] <"+connection.host+ " - " + connection.db+ "> ERROR: {!r}, ERROR NUMBER: {}".format(e, e.rowargs[0])


	# print "We created temporary table {0} and {1}... Now to write code for joining them!".format(table1.upper(), table2.upper())
	### TODO - close connection

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
	parser.add_argument("sqlfile", help="Location of SQL File, See the README for more information")
	parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
	global args
	args = parser.parse_args()
	print
	print "=" * 80
	print

	# read configuration and return a dictionary -------------------------------
	temp = "PARSING " + str(args.configfile) + "..."
	print
	print temp.center(80, " ")
	catalog_dict = get_config(args.configfile)
	print_pretty_dict(catalog_dict)

	# read sql commands for a list of tables -----------------------------------
	sql_commands = read_SQL(args.sqlfile)
	table_list = get_tables_real_names(sql_commands[0])
	print
	print "-" * 80
	print

	# read catalog for a list of node -----------------------------------
	print "READING CATALOG...".center(80, " ")
	print
	nodes_dict = read_catalog(catalog_dict, table_list);
	print_pretty_dict(nodes_dict)
	print
	print "-" * 80
	print


	# return a list of connections to all nodes --------------------------------
	# print "CREATING CONNECTIONS...".center(80, " ")
	# print

	# node_connections = get_connections(nodes_dict)
	# # if no connections were made, terminate the program, comment this out for testing
	# if len(node_connections) == 0:
	# 	print "Terminating due to connection failures..."
	# 	sys.exit()
	# print "# of connections:", str(len(node_connections))
	# print
	# for c in node_connections:
	# 	print "HOST: " + c.host + " DB: " + c.db + " " + str(c)
	# print
	# print "-" * 80
	# print

	# run the commands against the nodes ---------------------------------------
	print "EXECUTING SQL COMMANDS ON NODES...".center(80, " ")
	print
	node_connections = get_connections(nodes_dict)
	if len(node_connections) == 0:
		print "Terminating due to connection failures..."
		sys.exit()


	# a bit hardcoded for now ---
	if detect_join(sql_commands[0]):
		# a join was detected
		# if the partition method is range or hash, then run join
		if (nodes_dict['node1.partmtd'] == 1 or nodes_dict['node1.partmtd'] == 2):
			join_tables(nodes_dict, node_connections, table_list[0], table_list[1], sql_commands[0])
		else:
			print "TODO!!!! A NONPARTITION METHOD USED BUT JOIN WAS DETECTED!"
	else:
		# no join, no worries
		run_commmands_against_nodes(node_connections, sql_commands)

	print
	print "=" * 80
	print

if __name__ == "__main__":
	main()
