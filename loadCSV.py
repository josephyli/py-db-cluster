import argparse
import sys
import csv
import os
import pymysql.cursors
from pymysql import OperationalError
from ConfigParser import SafeConfigParser
from StringIO import StringIO

def loadCSV(configfilename, csvfilename):
	csv_dict = []
	with open(csvfilename, 'rb') as f:
		for row in csv.reader(f, delimiter=','):
			csv_dict.append(row)
	return csv_dict

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

			config_dict['catalog.tablename'] = cp.get('fakesection', 'tablename')

			config_dict['catalog.partition.method'] = cp.get('fakesection', 'partition.method')
			config_dict['catalog.partition.column'] = cp.get('fakesection', 'partition.column')

			# read the number of nodes
			numnodes = cp.getint('fakesection', 'numnodes')
			config_dict['catalog.numnodes'] = numnodes

			if (cp.get('fakesection', 'partition.method') == 'range'):
				# read node data and print out info
				for node in range(1, numnodes + 1):
					for parameter in ['param1', 'param2']:
						# test if candidate exists before adding to dictionary
						if cp.has_option('fakesection', "node" + str(node) + "." + parameter):
							config_dict["partition.node" + str(node) + "." + parameter] = cp.get('fakesection', "partition.node" + str(node) + ".param" + parameter)
						# else:
						# 	if candidate == "database":
						# 		config_dict["partition.node" + str(node) + ".database"] = cp.get('fakesection', "partition.node" + str(node) + ".hostname").rsplit('/', 1)[-1]
						# 	else: 
						# 		print "error: candidate not found"
			return config_dict
	else:
		print("No config file found at", configfilename)
		return null

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

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("configfile", help="Location of Config File, See the README for more information")
	parser.add_argument("csvfile", help="Location of CSV File, See the README for more information")
	args = parser.parse_args()

	nodes_dict = get_node_config(args.configfile)
	# print nodes_dict

	csv_dict = loadCSV(args.configfile, args.csvfile)
	for x in csv_dict:
		for y in x:
			print y

if __name__ == "__main__":
	main()
