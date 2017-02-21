import argparse
import csv
import os
import pymysql.cursors
import re
import sqlparse
import sys

from ConfigParser import SafeConfigParser
from StringIO import StringIO
from pymysql import OperationalError
from sqlparse.sql import Identifier
from sqlparse.sql import IdentifierList
from sqlparse.tokens import DML
from sqlparse.tokens import Keyword

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

def get_tables(sql_commands):
	from itertools import chain
	tables = []
	for command in sql_commands:
		tables.append(extract_tables(command))
	#extract the nested lists
	return list(chain.from_iterable(tables))

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
def get_catalog_config(configfilename):
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
		config_dict['catalog.numnodes'] = len(node_list)
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
				while True:
					row = cursor.fetchone()
					if row == None:
						print "No results found in " + connection.db
						break
					print(row)
			connection.commit()
			print "[JOB SUCCESSFUL] <"+connection.host+ " - " + connection.db+ ">"
			connection.close()
		except pymysql.MySQLError as e:
			print "[JOB FAILED] <"+connection.host+ " - " + connection.db+ "> ERROR: {!r}, ERROR NUMBER: {}".format(e, e.args[0])

def print_pretty_dict(idict):
	import json
	print json.dumps(idict, indent=1)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("sqlfile", help="Location of SQL File, See the README for more information")
	args = parser.parse_args()
	print
	print "=" * 80
	print

	# read configuration and return a dictionary -------------------------------
	temp = "PARSING " + str(args.configfile) + "..."
	print
	print temp.center(80, " ")
	catalog_dict = get_catalog_config(args.configfile)
	print_pretty_dict(catalog_dict)

	# read sql commands for a list of tables -----------------------------------
	sql_commands = read_SQL(args.sqlfile)
	table_list = get_tables(sql_commands)
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
	print "CREATING CONNECTIONS...".center(80, " ")
	print
	
	node_connections = get_connections(nodes_dict)
	# if no connections were made, terminate the program, comment this out for testing
	if len(node_connections) == 0:
		print "Terminating due to connection failures..."
		sys.exit()
	print "# of connections:", str(len(node_connections))
	print
	for c in node_connections:
		print "HOST: " + c.host + " DB: " + c.db + " " + str(c)
	print
	print "-" * 80
	print

	# run the commands against the nodes ---------------------------------------
	print "EXECUTING SQL COMMANDS ON NODES...".center(80, " ")
	print
	node_connections = get_connections(nodes_dict)
	if len(node_connections) == 0:
		print "Terminating due to connection failures..."
		sys.exit()
	run_commmands_against_nodes(node_connections, sql_commands)


	print
	print "=" * 80
	print

if __name__ == "__main__":
	main()