import argparse
import sys
import re
import os
import pymysql.cursors
from ConfigParser import SafeConfigParser
from StringIO import StringIO

# returns a list of sql commands as strings
def read_DDL(ddlfilename):
	f = open(ddlfilename, 'r')
	ddlfile = f.read()
	f.close()
	sql_commands = filter(None, ddlfile.split(';'))
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

			# read the number of nodes
			numnodes = cp.getint('fakesection', 'numnodes')
			config_dict['catalog.numnodes'] = numnodes

			# read node data and print out info
			for node in range(1, numnodes + 1):
				for candidate in ['driver', 'hostname', 'username', 'passwd']:
					# test if candidate exists before adding to dictionary
					if cp.has_option('fakesection', "node" + str(node) + "." + candidate):
						# print cp.get('fakesection', "node" + str(node) + "." + candidate)
						config_dict["node" + str(node) + "." + candidate] = cp.get('fakesection', "node" + str(node) + "." + candidate)
					else:
						print "error: candidate not found"
			return config_dict
	else:
		print("No config file found at", configfilename)
		return null

# stores metadata about the DDL in a catalog database
# using a list of tables that need to be created in the catalog
def update_catalog(config_dict, list_of_tables):
	hn = config_dict['catalog.hostname']
	usr = config_dict['catalog.username']
	pw = config_dict['catalog.passwd']
	d = config_dict['catalog.driver']

	sql = """ CREATE TABLE dtables (tname char(32),
			nodedriver char(64), 
			nodeurl char(128), 
			nodeuser char(16), 
			nodepasswd char(16), 
			partmtd int, 
			nodeid int, 
			partcol char(32), 
			partparam1 char(32),
			partparam2 char(32));
	"""

	# inserts into catalog the tables created for each node
	for table in list_of_tables:
		for i in range(config_dict["catalog.numnodes"]):
			try:
				hn = config_dict['node'+str(i + 1)+'.hostname']
				usr = config_dict['node'+str(i + 1)+'.username']
				pw = config_dict['node'+str(i + 1)+'.passwd']
				d = config_dict['node'+str(i + 1)+'.driver']

				sql += """INSERT INTO dtables 
					VALUES (\'%s\', \'%s\', \'%s\', \'%s\',\'%s\',
					NULL,%d,NULL,NULL,NULL);
					""" % (table,d,hn,usr,pw,i+1)
			except:
				print "update_catalog error"
	print sql


# returns a list of connections to all nodes
def get_connections(config_dict):
	connections = []
	for i in range(config_dict["catalog.numnodes"]):
		try:
			hn = config_dict['node'+str(i + 1)+'.hostname']
			usr = config_dict['node'+str(i + 1)+'.username']
			pw = config_dict['node'+str(i + 1)+'.passwd']
			d = config_dict['node'+str(i + 1)+'.driver']
			connections.append(connection = pymysql.connect(host=hn,
															user=usr,
															password=pw,
															db=d,
															charset='utf8mb4',
															cursorclass=pymysql.cursors.DictCursor))
		except:
			print "couldn't connect to node", i + 1
	return connections

# runs the list of commands against the list of connections
# later, this will implement multi-threading
def run_commmands_against_nodes(connections, sql_commands):
	
	# for every connection
	for connection in connections:
		try:
			with connection.cursor() as cursor:
					# execute every sql command
					for command in sql_commands:
						try:
							print connection, "executing ", command
							print
							cursor.execute(command.strip() + ';')
						except OperationalError, msg:
							print "Command skipped: ", msg
							connection.commit()
		except e:
			print e
		finally:
			connection.close()

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("ddlfile", help="Location of DDL File, See the README for more information")
	args = parser.parse_args()
	print
	print "=" * 80
	print
	print

	# read configuration and return a dictionary -------------------------------
	print "parsing", args.configfile, "into a dict..."
	nodes_dict = get_node_config(args.configfile)
	print "-" * 80
	print

	# return a list of connections to all nodes --------------------------------
	print "creating connections from..."
	print nodes_dict
	node_connections = get_connections(nodes_dict)
	print "-" * 80
	print

	# read DDL and return a list of sql commands -------------------------------
	print "parsing", args.ddlfile, "into sql commands..."
	sql_commands = read_DDL(args.ddlfile)

	# list of tables is used to update catalog with metadata
	list_of_tables = []
	for command in sql_commands:
		if command.split()[0].upper() == "CREATE":
			list_of_tables.append((re.split('\s|\(',command)[2]))
	print "list of tables needed:" 
	print list_of_tables
	print "resulting sql commands"
	print sql_commands

	update_catalog(nodes_dict,list_of_tables)
	print "-" * 80
	print

	# run the commands against the nodes ---------------------------------------
	print "running all known sql commands against all connections..."
	run_commmands_against_nodes(node_connections, sql_commands)
	print "-" * 80
	print

	print
	print "=" * 80
	print

if __name__ == "__main__":
	main()
