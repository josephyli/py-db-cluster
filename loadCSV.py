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
	with open(csvfilename, 'rb') as f:
		for row in csv.reader(f, delimiter=','):
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

			config_dict['catalog.partition.column'] = cp.get('fakesection', 'partition.column')

			# read the number of nodes... if it's listed
			if cp.has_option('fakesection', 'numnodes'):
				numnodes = cp.getint('fakesection', 'numnodes')
				config_dict['catalog.numnodes'] = numnodes
			
				# read depending on partition method
				if (partition_method_string == 'notpartition'):
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

				elif (partition_method_string == 'range'):
					# read node data and print out info
					for node in range(1, numnodes + 1):
						for parameter in ['param1', 'param2']:
							# test if candidate exists before adding to dictionary
							if cp.has_option('fakesection', "partition.node" + str(node) + "." + parameter):
								config_dict["partition.node" + str(node) + "." + parameter] = cp.getint('fakesection', "partition.node" + str(node) + "." + parameter)
					return config_dict


			elif (cp.get('fakesection', 'partition.method') == 'hash'):
				print "Hash method selected!!!"
				return config_dict
	else:
		print("No config file found at", configfilename)
		return null

def print_pretty_dict(idict):
	import json
	print json.dumps(idict, indent=1)

# returns a list of connections to all nodes
# def get_connections(config_dict):
# 	connections = []

# 	for i in range(config_dict["catalog.numnodes"]):
# 		try:
# 			hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['node'+str(i + 1)+'.hostname'] )[0]
# 			usr = config_dict['node'+str(i + 1)+'.username']
# 			pw = config_dict['node'+str(i + 1)+'.passwd']
# 			db = config_dict['node'+str(i + 1)+'.database']
# 			connections.append(pymysql.connect(host=hn,
# 											user=usr,
# 											password=pw,
# 											db=db,
# 											charset='utf8mb4',
# 											cursorclass=pymysql.cursors.DictCursor))
# 		except:
# 			print "[NODE", i +  1, "CONNECTION FAILED]:"
# 			print "hostname:".rjust(12), re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['node'+str(i + 1)+'.hostname'] )[0]
# 			print "username:".rjust(12), config_dict['node'+str(i + 1)+'.username']
# 			print "password:".rjust(12), config_dict['node'+str(i + 1)+'.passwd']
# 			print "database:".rjust(12), config_dict['node'+str(i + 1)+'.database']
# 			print
# 	return connections

# update node data from catalog
def update_catalog_with_partitions(config_dict):
	cat_hn = re.findall( r'[0-9]+(?:\.[0-9]+){3}', config_dict['catalog.hostname'] )[0]
	cat_usr = config_dict['catalog.username']
	cat_pw = config_dict['catalog.passwd']
	cat_dr = config_dict['catalog.driver']
	cat_db = config_dict['catalog.database']
	
	tablename = config_dict['catalog.tablename']
	pm = config_dict['catalog.partition.method']
	pc = config_dict['catalog.partition.column']

	sql = []

	# prepares the sql statement to insert into catalog the tables in each node
	for nodeid in range(1, config_dict["catalog.numnodes"] + 1):
			# hn = config_dict['node'+str(i + 1)+'.hostname']
			# usr = config_dict['node'+str(i + 1)+'.username']
			# pw = config_dict['node'+str(i + 1)+'.passwd']
			# dr = config_dict['node'+str(i + 1)+'.driver']
			p1 = config_dict['partition.node'+str(nodeid)+'.param1']
			p2 = config_dict['partition.node'+str(nodeid)+'.param2']

			sql.append("UPDATE dtables SET partmtd = %d, partcol = \'%s\', partparam1 = %d, partparam2 = %d WHERE tname=\'%s\' AND nodeid = %d; " % (pm, pc, p1, p2, tablename, nodeid))

	# connect and execute the sql statement
	connection = pymysql.connect(host=cat_hn,
				user=cat_usr,
				password=cat_pw,
				db=cat_db,
				charset='utf8mb4',
				cursorclass=pymysql.cursors.DictCursor)
	try:
		# connect and execute the sql statement
		connection = pymysql.connect(host=cat_hn,
					user=cat_usr,
					password=cat_pw,
					db=cat_db,
					charset='utf8mb4',
					cursorclass=pymysql.cursors.DictCursor)

		# print "[SUCCESSFUL CATALOG CONNECTION] <"+connection.host+" - "+connection.db+">", connection
		# print

		with connection.cursor() as cursor:
			# execute every sql command
			for command in sql:
				try:
					# print command
					# print
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
	return node_list

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("csvfile", help="Location of CSV File, See the README for more information")
	args = parser.parse_args()

	# read the config file into a dictionary
	config_dict = get_partition_config(args.configfile)

	node_list = update_catalog_with_partitions(config_dict)
	print_pretty_dict(node_list)

	# return a list of connections to all nodes
	# node_connections = get_connections(nodes_dict)
	# if len(node_connections) == 0:
		# print "Terminating due to connection failures..."
		# sys.exit()

	# read the csv file into a list
	csv_list = loadCSV(args.configfile, args.csvfile)
	
	# print "Printing csv list:"
	# for x in csv_list:
	# 	for y in x:
	# 		print y
	
	#update the catalog using the stored table name in the node_dict
	# print nodes_dict['catalog.hostname']
	
	

if __name__ == "__main__":
	main()
