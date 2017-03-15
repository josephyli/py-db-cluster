import argparse
import csv
import os
import pymysql.cursors
import re
import sys

from ConfigParser import SafeConfigParser
from StringIO import StringIO
from pymysql import OperationalError

def loadCSV(configfilename, csvfilename):
	csv_list = []
	with open(csvfilename, 'rb') as csvfile:
		dialect = csv.Sniffer().sniff(csvfile.read(), delimiters=',|;')
		csvfile.seek(0)
		reader = csv.reader(csvfile, dialect)

		for row in reader:
			csv_list.append(row)
	return csv_list

# returns a dict with all nodes information
# responsible for parsing the config file
def get_partition_config(configfilename):
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
							config_dict["catalog.numnodes"] = numnodes
					except OperationalError, msg:
						print "Command skipped: ", msg
						connection.commit()

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
				if numnodes != node -1:
					print "Error! dtables's number of nodes does not match the partitioning. Exiting..."
					sys.exit(1)
				return config_dict

			elif (cp.get('fakesection', 'partition.method') == 'hash'):
				print "Loading the CSV based hash partitioning"
				config_dict['catalog.numnodes'] = cp.getint('fakesection', 'partition.param1')
				config_dict['partition.column'] = cp.get('fakesection', 'partition.column')
				config_dict['partition.param1'] = cp.getint('fakesection', 'partition.param1')
				if numnodes != config_dict['partition.param1']:
					print "Error! dtables's number of nodes does not match the partitioning. Exiting..."
					sys.exit(1)
				return config_dict
	else:
		print("No config file found at", configfilename)
		return null

def print_pretty_dict(idict):
	import json
	print json.dumps(idict, indent=1)

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
		# config_dict['catalog.numnodes'] = len(node_list)
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

	print "Index",partition_index,"of",columns,"is the partition column"

	try:
		# construct the sql_statement
		values = ', '.join(["%s" for i in columns])
		sql_statement = "INSERT INTO " + config_dict['catalog.tablename'].upper() + " VALUES ({a})".format(a=values)
		args = ()

		for i in range(len(csv_list)):		
			# convert each row into a tuple to be passed
			args = tuple(csv_list[i]) 

			nodeid = int(csv_list[i][partition_index]) % int(config_dict['catalog.numnodes'])
			with node_connections[nodeid].cursor() as cursor:
				cursor.execute(sql_statement, args)
				res = cursor.fetchone()
				node_connections[nodeid].commit()
				print "data committed to node " + str(nodeid + 1)
	except pymysql.MySQLError as e:
		print e
	finally:
		cursor.close()
	



def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("csvfile", help="Location of CSV File, See the README for more information")
	args = parser.parse_args()

	if args.csvfile[-4:].lower() != ".csv":
		print "Are you certain you are providing a CSV file?"
		sys.exit(1)

	# read the config file into a dictionary
	print "READING CONFIG FILE...".center(80, " ")
	print
	config_dict = get_partition_config(args.configfile)
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
	for c in node_connections:
		print "HOST: " + c.host + " DB: " + c.db + " " + str(c)
	print
	print "-" * 80
	print

	# read the csv file into a list
	csv_list = loadCSV(args.configfile, args.csvfile)

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

	#update the catalog using the stored table name in the node_dict
	# print nodes_dict['catalog.hostname']


if __name__ == "__main__":
	main()
