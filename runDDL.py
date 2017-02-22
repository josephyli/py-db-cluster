import argparse
import os
import pymysql.cursors
import re
import sys

from ConfigParser import SafeConfigParser
from StringIO import StringIO
from pymysql import OperationalError

# returns a list of sql commands as strings
def read_DDL(ddlfilename):
	f = open(ddlfilename, 'r')
	ddlfile = f.read()
	f.close()
	temp = filter(None, ddlfile.split(';'))
	sql_commands = []
	# filter out white space from file input
	for c in temp:
		if c != "\n":
			sql_commands.append(c)
	return sql_commands

# returns a dict with all nodes information
# responsible for parsing the config file
def get_node_config(configfilename):
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

			# read the number of nodes
			numnodes = cp.getint('fakesection', 'numnodes')
			config_dict['catalog.numnodes'] = numnodes

			# read node data and print out info
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

# stores metadata about the DDL in a catalog database
# using a list of tables that need to be created in the catalog
def update_catalog(config_dict, table_list):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']

	sql = ["DROP TABLE IF EXISTS dtables", "CREATE TABLE dtables (tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));"]

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

		print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
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
		except pymysql.MySQLError as e:
			print "[NODE", i +  1, "CONNECTION FAILED]:"
			print "hostname:".rjust(12), re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['node'+str(i + 1)+'.hostname'] )[0]
			print "username:".rjust(12), config_dict['node'+str(i + 1)+'.username']
			print "password:".rjust(12), config_dict['node'+str(i + 1)+'.passwd']
			print "database:".rjust(12), config_dict['node'+str(i + 1)+'.database']
			print 'Got error {!r}, errno is {}'.format(e, e.args[0])
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
	parser.add_argument("ddlfile", help="Location of DDL File, See the README for more information")
	args = parser.parse_args()
	print
	print "=" * 80
	print

	# read configuration and return a dictionary -------------------------------
	temp = "PARSING " + str(args.configfile) + "..."
	print
	print temp.center(80, " ")
	nodes_dict = get_node_config(args.configfile)
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

	# read DDL and return a list of sql commands -------------------------------
	print "PARSING SQL COMMANDS...".center(80, " ")
	print
	sql_commands = read_DDL(args.ddlfile)
	# list of tables is used to update catalog with metadata
	table_list = []
	for command in sql_commands:
		if command.split()[0].upper() == "CREATE":
			table_list.append((re.split('\s|\(',command)[2]))
	print "[SQL COMMANDS]:"
	for s in sql_commands:
		print s.strip()
	print
	print "TABLES:"
	print table_list
	print
	print "-" * 80
	print

	# update catalog  ----------------------------------------------------------
	print "UPDATING CATALOG...".center(80, " ")
	print
	update_catalog(nodes_dict,table_list)
	print
	print "-" * 80
	print

	# run the commands against the nodes ---------------------------------------
	print "EXECUTING SQL COMMANDS ON NODES...".center(80, " ")
	print
	run_commmands_against_nodes(node_connections, sql_commands)

	print
	print "=" * 80
	print

if __name__ == "__main__":
	main()
