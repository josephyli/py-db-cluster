import argparse
import sys
import re
import os
import pymysql.cursors
from ConfigParser import SafeConfigParser
from StringIO import StringIO

# returns a list of ddl commands as strings
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

# returns a list of sql commands as strings
def read_SQL(sqlfilename):
	f = open(sqlfilename, 'r')
	sqlfile = f.read()
	f.close()
	temp = filter(None, sqlfile.split(';'))
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

	sql = ["DROP TABLE IF EXISTS dtables; CREATE TABLE dtables (tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));"]

	# prepares the sql statement to insert into catalog the tables in each node
	for table in table_list:
		for i in range(config_dict["catalog.numnodes"]):
				hn = config_dict['node'+str(i + 1)+'.hostname']
				usr = config_dict['node'+str(i + 1)+'.username']
				pw = config_dict['node'+str(i + 1)+'.passwd']
				dr = config_dict['node'+str(i + 1)+'.driver']

				sql.append("INSERT INTO dtables VALUES (\'%s\', \'%s\', \'%s\', \'%s\',\'%s\', NULL,%d,NULL,NULL,NULL);" % (table,dr,hn,usr,pw,i+1))
	# connect and execute the sql statement
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
					print connection, "updating catalog: executing ", command
					print
					cursor.execute(command.strip() + ';')
				except OperationalError, msg:
					print "Command skipped: ", msg
					connection.commit()

	except:
			print "couldn't connect to catalog"

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
			for command in sql_commands:
				print "[TABLE CREATED]<",connection.host+"> for db <",connection.db+">"
				list_of_threads.append(Thread(target=run_sql_command_against_node, args=(connection, command)))
	print
	# start up all jobs
	for t in list_of_threads:
		t.start()
	# wait for all jobs to complete before moving on
	while active_count() > 1:
		time.sleep(1)


def run_sql_command_against_node(connection, sql_command):
	with connection.cursor() as cursor:
		print "-    -    " * 8
		print "[", connection.host, "]"
		print sql_command
		try:
			cursor.execute(sql_command.strip() + ';')
			connection.commit()
			print "Command successful"
		except pymysql.MySQLError as e:
			print 'Got error {!r}, errno is {}'.format(e, e.args[0])
		print "-    -    " * 8
		print

def print_pretty_dict(idict):
	import json
	print json.dumps(idict, indent=1)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("ddlfile", help="Location of DDL File, See the README for more information")
	parser.add_argument("sqlfile", help="Location of SQL File, See the README for more information")
	args = parser.parse_args()
	print
	print "=" * 80
	print

	# read configuration and return a dictionary -------------------------------
	# print "parsing", args.configfile, "into a dict..."
	nodes_dict = get_node_config(args.configfile)
	# print_pretty_dict(nodes_dict)
	# print
	# print "-" * 80
	# print

	# return a list of connections to all nodes --------------------------------
	print "Creating Connections..."
	node_connections = get_connections(nodes_dict)
	# if no connections were made, terminate the program, comment this out for testing
	if len(node_connections) == 0:
		print "Terminating due to connection failures..."
		sys.exit()
	print "Number of Connections:", str(len(node_connections))
	for c in node_connections:
		print c.host
	print
	print "-" * 80
	print

	# read DDL and return a list of sql commands -------------------------------
	print "parsing ", args.ddlfile, "into sql commands..."
	sql_commands = read_DDL(args.ddlfile)
	# list of tables is used to update catalog with metadata
	table_list = []
	for command in sql_commands:
		if command.split()[0].upper() == "CREATE":
			table_list.append((re.split('\s|\(',command)[2]))
	# print "list of tables needed:"
	# print table_list
	# print "resulting sql commands"
	# print sql_commands
	update_catalog(nodes_dict,table_list)
	print
	print "-" * 80
	print

	# run the commands against the nodes ---------------------------------------
	print "creating tables in nodes according to ddlfile..."
	print
	run_commmands_against_nodes(node_connections, sql_commands)

	# run the commands against the nodes ---------------------------------------
	# print "running all known sql commands against all connections..."
	# print
	# sql_commands = read_SQL(args.sqlfile)
	# run_commmands_against_nodes(node_connections, sql_commands)

	print
	print "=" * 80
	print

if __name__ == "__main__":
	main()
